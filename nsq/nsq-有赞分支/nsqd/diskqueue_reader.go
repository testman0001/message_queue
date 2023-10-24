package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/levellogger"
)

const (
	MAX_POSSIBLE_MSG_SIZE = 1 << 28
)

var readBufferSize = 1024 * 4

var errInvalidMetaFileData = errors.New("invalid meta file data")
var diskMagicEndBytes = []byte{0xae, 0x83}
var testCrash = false

var (
	ErrReadQueueAlreadyCleaned = errors.New("the queue position has been cleaned")
	ErrConfirmSizeInvalid      = errors.New("Confirm data size invalid.")
	ErrConfirmCntInvalid       = errors.New("Confirm message count invalid.")
	ErrMoveOffsetInvalid       = errors.New("move offset invalid")
	ErrMoveOffsetOverflowed    = errors.New("disk reader move offset overflowed the end")
	ErrOffsetTypeMismatch      = errors.New("offset type mismatch")
	ErrReadQueueCountMissing   = errors.New("read queue count info missing")
	ErrReadEndOfQueue          = errors.New("read to the end of queue")
	ErrInvalidReadable         = errors.New("readable data is invalid")
	ErrReadEndChangeToOld      = errors.New("queue read end change to old without reload")
	ErrExiting                 = errors.New("exiting")
)

var FileNumV2Seq = 999990

type diskQueueOffset struct {
	FileNum int64
	Pos     int64
}

type diskQueueOffsetInfo struct {
	EndOffset  diskQueueOffset
	virtualEnd BackendOffset
}

func (d *diskQueueOffsetInfo) Offset() BackendOffset {
	return d.virtualEnd
}

type diskQueueEndInfo struct {
	diskQueueOffsetInfo
	totalMsgCnt int64
}

func (d *diskQueueEndInfo) Offset() BackendOffset {
	return d.virtualEnd
}

func (d *diskQueueEndInfo) TotalMsgCnt() int64 {
	return atomic.LoadInt64(&d.totalMsgCnt)
}

func (d *diskQueueEndInfo) IsSame(other BackendQueueEnd) bool {
	if otherDiskEnd, ok := other.(*diskQueueEndInfo); ok {
		return *d == *otherDiskEnd
	}
	return false
}

func (d *diskQueueOffset) GreatThan(o *diskQueueOffset) bool {
	if d.FileNum > o.FileNum {
		return true
	}
	if d.FileNum == o.FileNum && d.Pos > o.Pos {
		return true
	}
	return false
}

// diskQueueReader implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueReader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	readQueueInfo diskQueueEndInfo
	queueEndInfo  diskQueueEndInfo
	// left message number for read
	depth     int64
	depthSize int64

	sync.RWMutex

	// instantiation time metadata
	readerMetaName  string
	readFrom        string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	syncEvery       int64 // number of writes per fsync
	exitFlag        int32
	needSync        bool

	confirmedQueueInfo diskQueueEndInfo

	readFile   *os.File
	readBuffer *bytes.Buffer

	exitChan        chan int
	autoSkipError   bool
	waitingMoreData int32
	metaStorage     IMetaStorage
	kvTopic         *KVTopic
}

func newDiskQueueReaderWithFileMeta(readFrom string, metaname string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, readEnd BackendQueueEnd, autoSkip bool) BackendQueueReader {
	metaStorage := &fileMetaStorage{}
	return newDiskQueueReader(readFrom, metaname, dataPath, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery,
		syncTimeout, readEnd, autoSkip, metaStorage, nil, true)
}

// newDiskQueue instantiates a new instance of diskQueueReader, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueReader(readFrom string, metaname string,
	dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration,
	readEnd BackendQueueEnd, autoSkip bool,
	metaStorage IMetaStorage, kvTopic *KVTopic, forceReload bool) BackendQueueReader {

	d := diskQueueReader{
		readFrom:        readFrom,
		readerMetaName:  metaname,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		exitChan:        make(chan int),
		syncEvery:       syncEvery,
		autoSkipError:   autoSkip,
		kvTopic:         kvTopic,
		readBuffer:      bytes.NewBuffer(make([]byte, 0, readBufferSize)),
	}
	if metaStorage == nil {
		// fallback
		metaStorage = &fileMetaStorage{}
	}
	d.metaStorage = metaStorage

	// init the channel to end, so if any new channel without meta will be init to read at end
	if diskEnd, ok := readEnd.(*diskQueueEndInfo); ok {
		d.confirmedQueueInfo = *diskEnd
		d.readQueueInfo = d.confirmedQueueInfo
		d.queueEndInfo = *diskEnd
		d.updateDepth()
	} else {
		nsqLog.Logf("diskqueue(%s) read end not valid %v",
			d.readFrom, readEnd)
	}
	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData %v - %s",
			d.readFrom, d.readerMetaName, err)
	}
	// need update queue end with new readEnd, since it may read old from meta file
	// force reload end should be true after the topic end is fixed by fix mode.
	// Do not force update if under need fix state since the end may be wrong.
	d.UpdateQueueEnd(readEnd, forceReload)

	return &d
}

func getQueueSegmentEnd(dataRoot string, readFrom string, offset diskQueueOffset) (int64, error) {
	curFileName := GetQueueFileName(dataRoot, readFrom, offset.FileNum)
	f, err := os.Stat(curFileName)
	if err != nil {
		return 0, err
	}
	return f.Size(), nil
}

func (d *diskQueueReader) getCurrentFileEnd(offset diskQueueOffset) (int64, error) {
	return getQueueSegmentEnd(d.dataPath, d.readFrom, offset)
}

// Depth returns the depth of the queue
func (d *diskQueueReader) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

func (d *diskQueueReader) DepthSize() int64 {
	return atomic.LoadInt64(&d.depthSize)
}

func (d *diskQueueReader) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	e := d.queueEndInfo
	d.RUnlock()
	return &e
}

func (d *diskQueueReader) GetQueueConfirmed() BackendQueueEnd {
	d.RLock()
	e := d.confirmedQueueInfo
	d.RUnlock()
	return &e
}

func (d *diskQueueReader) GetQueueCurrentRead() BackendQueueEnd {
	d.RLock()
	ret := d.readQueueInfo
	d.RUnlock()
	return &ret
}

// force reopen is used to avoid the read prefetch by OS read the previously rollbacked data by writer.
// we need make sure this since we may read/write on the same file in different thread.
func (d *diskQueueReader) UpdateQueueEnd(e BackendQueueEnd, forceReload bool) (bool, error) {
	end, ok := e.(*diskQueueEndInfo)
	if !ok || end == nil {
		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.Logf("%v got nil end while update queue end", d.readerMetaName)
		}
		return false, nil
	}
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return false, ErrExiting
	}
	return d.internalUpdateEnd(end, forceReload)
}

func (d *diskQueueReader) Delete() error {
	return d.exit(true)
}

// Close cleans up the queue and persists metadata
func (d *diskQueueReader) Close() error {
	return d.exit(false)
}

func (d *diskQueueReader) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1
	close(d.exitChan)
	nsqLog.Logf("diskqueue(%s) exiting ", d.readerMetaName)
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.syncAll(true)
	if deleted {
		d.skipToEndofQueue()
		d.metaStorage.Remove(d.metaDataFileName(false))
		d.metaStorage.Remove(d.metaDataFileName(true))
		nsqLog.Logf("diskqueue(%s) remove new metadata file - %v", d.readerMetaName, d.metaDataFileName(true))
	}
	return nil
}

func (d *diskQueueReader) ConfirmRead(offset BackendOffset, cnt int64) error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return ErrExiting
	}
	oldConfirm := d.confirmedQueueInfo.Offset()
	err := d.internalConfirm(offset, cnt)
	if oldConfirm != d.confirmedQueueInfo.Offset() {
		d.needSync = true
	}
	return err
}

func (d *diskQueueReader) Flush(fsync bool) {
	d.Lock()
	if d.exitFlag == 1 {
		d.Unlock()
		return
	}
	confirmed := d.confirmedQueueInfo
	qe := d.queueEndInfo
	needSync := d.needSync
	d.needSync = false
	d.Unlock()
	if needSync {
		d.persistMetaData(fsync, confirmed, qe)
	}
}

func (d *diskQueueReader) ResetReadToConfirmed() (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return nil, ErrExiting
	}
	old := d.confirmedQueueInfo.Offset()
	skiperr := d.internalSkipTo(d.confirmedQueueInfo.Offset(), d.confirmedQueueInfo.TotalMsgCnt(), false)
	if skiperr == nil {
		if old != d.confirmedQueueInfo.Offset() {
			d.needSync = true
			if d.syncEvery == 1 {
				d.syncAll(false)
			}
		}
	}

	e := d.confirmedQueueInfo
	return &e, skiperr
}

func (d *diskQueueReader) ResetReadToQueueStart(start diskQueueEndInfo) error {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return ErrExiting
	}
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readBuffer.Reset()

	nsqLog.Warningf("reset read to start: %v, %v to: %v", d.readQueueInfo, d.confirmedQueueInfo, start)
	d.confirmedQueueInfo = start
	d.readQueueInfo = d.confirmedQueueInfo
	d.updateDepth()
	d.needSync = true
	if d.syncEvery == 1 {
		d.syncAll(false)
	}
	return nil
}

// reset can be set to the old offset before confirmed, skip can only skip forward confirmed.
func (d *diskQueueReader) ResetReadToOffset(offset BackendOffset, cnt int64) (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return nil, ErrExiting
	}
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readBuffer.Reset()

	old := d.confirmedQueueInfo.Offset()
	if offset < d.confirmedQueueInfo.Offset() {
		nsqLog.Debugf("reset from: %v, %v to: %v:%v", d.readQueueInfo, d.confirmedQueueInfo, offset, cnt)
	}
	err := d.internalSkipTo(offset, cnt, offset < old)
	if err == nil {
		if old != d.confirmedQueueInfo.Offset() {
			d.needSync = true
			if d.syncEvery == 1 {
				d.syncAll(false)
			}
		}
	}

	e := d.confirmedQueueInfo
	return &e, err
}

func (d *diskQueueReader) ResetLastReadOne(offset BackendOffset, cnt int64, lastMoved int32) {
	d.Lock()
	defer d.Unlock()
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	if d.readQueueInfo.EndOffset.Pos < int64(lastMoved) {
		return
	}
	d.readBuffer.Reset()
	d.readQueueInfo.EndOffset.Pos -= int64(lastMoved)
	d.readQueueInfo.virtualEnd = offset
	if cnt > 0 || (offset == 0 && cnt == 0) {
		atomic.StoreInt64(&d.readQueueInfo.totalMsgCnt, cnt)
	}
}

func (d *diskQueueReader) SkipReadToOffset(offset BackendOffset, cnt int64) (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return nil, ErrExiting
	}
	old := d.confirmedQueueInfo.Offset()
	skiperr := d.internalSkipTo(offset, cnt, false)
	if skiperr == nil {
		if old != d.confirmedQueueInfo.Offset() {
			d.needSync = true
			if d.syncEvery == 1 {
				d.syncAll(false)
			}
		}
	}

	e := d.confirmedQueueInfo
	return &e, skiperr
}

func (d *diskQueueReader) SkipReadToEnd() (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return nil, ErrExiting
	}
	old := d.confirmedQueueInfo.Offset()
	skiperr := d.internalSkipTo(d.queueEndInfo.Offset(), d.queueEndInfo.TotalMsgCnt(), false)
	if skiperr == nil {
		// it may offset unchanged but the queue end file skip to next
		// so we need to change all to end.
		d.confirmedQueueInfo = d.queueEndInfo
		d.readQueueInfo = d.queueEndInfo
		if old != d.confirmedQueueInfo.Offset() {
			d.needSync = true
			if d.syncEvery == 1 {
				d.syncAll(false)
			}
		}
	}

	e := d.confirmedQueueInfo
	return &e, skiperr
}

// waiting more data if all data has been confirmed to consumed
func (d *diskQueueReader) IsWaitingMoreData() bool {
	return atomic.LoadInt32(&d.waitingMoreData) == 1
}

// read to end means no more data on disk, but maybe still waiting ack in memory
func (d *diskQueueReader) isReadToEnd() bool {
	if d.IsWaitingMoreData() {
		return true
	}
	d.RLock()
	hasDiskData := d.queueEndInfo.EndOffset.GreatThan(&d.readQueueInfo.EndOffset)
	d.RUnlock()
	return !hasDiskData
}

func (d *diskQueueReader) SkipToNext() (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return nil, ErrExiting
	}
	err := d.skipToNextFile()
	if err != nil {
		return nil, err
	}
	e := d.confirmedQueueInfo
	return &e, nil
}

func (d *diskQueueReader) TryReadOne() (ReadResult, bool) {
	d.Lock()
	defer d.Unlock()
	for {
		if d.queueEndInfo.EndOffset.GreatThan(&d.readQueueInfo.EndOffset) && d.queueEndInfo.Offset() > d.readQueueInfo.Offset() {
			dataRead := d.readOne()
			rerr := dataRead.Err
			if rerr != nil {
				nsqLog.LogErrorf("reading from diskqueue(%s) at %d of %s - %s, current end: %v",
					d.readerMetaName, d.readQueueInfo, d.fileName(d.readQueueInfo.EndOffset.FileNum), dataRead.Err, d.queueEndInfo)
				if rerr != ErrReadQueueCountMissing && d.autoSkipError {
					d.handleReadError()
					continue
				}
			}
			if d.kvTopic != nil {
				rmsg, err := d.kvTopic.GetMsgRawByCnt(dataRead.CurCnt - 1)
				if err != nil {
					nsqLog.Logf("reading from kv failed: %v, %v, %v", dataRead.CurCnt, dataRead.Offset, err.Error())
					dataRead.Err = err
				} else {
					if !bytes.Equal(rmsg, dataRead.Data) {
						nsqLog.LogWarningf("reading from kv not matched: %v, %v, %v, %v", dataRead.CurCnt, dataRead.Offset, rmsg, dataRead.Data)
						dataRead.Err = errors.New("kv data not matched")
					}
				}
			}
			return dataRead, true
		} else {
			if nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("reading from diskqueue(%s) no more data at pos: %v, queue end: %v, confirmed: %v",
					d.readerMetaName, d.readQueueInfo, d.queueEndInfo, d.confirmedQueueInfo)
			}
			return ReadResult{}, false
		}
	}
}

func (d *diskQueueReader) updateDepth() {
	newDepth := int64(0)
	if d.confirmedQueueInfo.EndOffset.FileNum > d.queueEndInfo.EndOffset.FileNum {
		atomic.StoreInt64(&d.depth, 0)
		atomic.StoreInt64(&d.depthSize, 0)
	} else {
		newDepthSize := int64(d.queueEndInfo.Offset() - d.confirmedQueueInfo.Offset())
		atomic.StoreInt64(&d.depthSize, newDepthSize)
		newDepth = int64(d.queueEndInfo.TotalMsgCnt() - d.confirmedQueueInfo.TotalMsgCnt())
		atomic.StoreInt64(&d.depth, newDepth)
		if newDepthSize == 0 {
			if newDepth != 0 {
				nsqLog.Warningf("the confirmed info conflict with queue end: %v, %v", d.confirmedQueueInfo, d.queueEndInfo)
				d.confirmedQueueInfo = d.queueEndInfo
			}
			newDepth = 0
		}
	}
	if newDepth == 0 {
		atomic.StoreInt32(&d.waitingMoreData, 1)
	}
}

func (d *diskQueueReader) getVirtualOffsetDistance(prev diskQueueOffset, next diskQueueOffset) (BackendOffset, error) {
	diff := int64(0)
	if prev.GreatThan(&next) {
		return BackendOffset(diff), ErrMoveOffsetInvalid
	}
	if prev.FileNum == next.FileNum {
		diff = next.Pos - prev.Pos
		return BackendOffset(diff), nil
	}

	fsize, err := d.getCurrentFileEnd(prev)
	if err != nil {
		return BackendOffset(diff), err
	}
	left := fsize - prev.Pos
	prev.FileNum++
	prev.Pos = 0
	vdiff := BackendOffset(0)
	vdiff, err = d.getVirtualOffsetDistance(prev, next)
	return BackendOffset(int64(vdiff) + left), err
}

func stepOffset(dataRoot string, readFrom string, cur diskQueueEndInfo, step BackendOffset, maxStep diskQueueEndInfo) (diskQueueOffset, error) {
	newOffset := cur
	var err error
	if cur.EndOffset.FileNum > maxStep.EndOffset.FileNum {
		return newOffset.EndOffset, fmt.Errorf("offset invalid: %v , %v", cur, maxStep)
	}
	if step == 0 {
		return newOffset.EndOffset, nil
	}
	// TODO: maybe should handle step back to queue cleaned start
	virtualCur := cur.Offset()
	maxVirtual := maxStep.Offset()
	if virtualCur+step < 0 {
		// backward exceed
		return newOffset.EndOffset, fmt.Errorf("move offset step %v from %v to exceed begin", step, virtualCur)
	} else if virtualCur+step > maxVirtual {
		// forward exceed
		return newOffset.EndOffset, fmt.Errorf("move offset step %v from %v exceed max: %v", step, virtualCur, maxVirtual)
	}
	if step < 0 {
		// handle backward
		step = 0 - step
		for step > BackendOffset(newOffset.EndOffset.Pos) {
			newOffset.virtualEnd -= BackendOffset(newOffset.EndOffset.Pos)
			step -= BackendOffset(newOffset.EndOffset.Pos)
			nsqLog.Logf("step read back to previous file: %v, %v", step, newOffset)
			newOffset.EndOffset.Pos = 0
			if newOffset.EndOffset.FileNum < 1 {
				nsqLog.Logf("reset read acrossed the begin %v, %v", step, newOffset)
				return newOffset.EndOffset, ErrReadQueueAlreadyCleaned
			}
			var f os.FileInfo
			f, err = os.Stat(GetQueueFileName(dataRoot, readFrom, newOffset.EndOffset.FileNum-1))
			if err != nil {
				nsqLog.LogErrorf("stat data file error %v, %v: %v", step, newOffset, err)
				if os.IsNotExist(err) {
					return newOffset.EndOffset, ErrReadQueueAlreadyCleaned
				}
				return newOffset.EndOffset, err
			}
			newOffset.EndOffset.FileNum--
			newOffset.EndOffset.Pos = f.Size()
		}
		newOffset.EndOffset.Pos -= int64(step)
		newOffset.virtualEnd -= BackendOffset(step)
		return newOffset.EndOffset, nil
	}
	for {
		end := int64(0)
		if newOffset.EndOffset.FileNum < maxStep.EndOffset.FileNum {
			end, err = getQueueSegmentEnd(dataRoot, readFrom, newOffset.EndOffset)
			if err != nil {
				return newOffset.EndOffset, err
			}
		} else {
			end = maxStep.EndOffset.Pos
		}
		diff := end - newOffset.EndOffset.Pos
		old := newOffset.EndOffset
		if step > BackendOffset(diff) {
			newOffset.EndOffset.FileNum++
			newOffset.EndOffset.Pos = 0
			if newOffset.EndOffset.GreatThan(&maxStep.EndOffset) {
				return old, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			step -= BackendOffset(diff)
			newOffset.virtualEnd += BackendOffset(diff)
			if step == 0 {
				return newOffset.EndOffset, nil
			}
		} else {
			newOffset.EndOffset.Pos += int64(step)
			if newOffset.EndOffset.GreatThan(&maxStep.EndOffset) {
				return old, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			newOffset.virtualEnd += BackendOffset(step)
			return newOffset.EndOffset, nil
		}
	}
}

func (d *diskQueueReader) internalConfirm(offset BackendOffset, cnt int64) error {
	if int64(offset) == -1 {
		d.confirmedQueueInfo = d.readQueueInfo
		d.updateDepth()
		nsqLog.LogDebugf("confirmed to end: %v", d.confirmedQueueInfo)
		return nil
	}
	if offset <= d.confirmedQueueInfo.Offset() {
		nsqLog.LogDebugf("already confirmed to : %v", d.confirmedQueueInfo.Offset())
		return nil
	}
	if offset > d.readQueueInfo.Offset() {
		nsqLog.LogErrorf("%v confirm exceed read: %v, %v", d.readerMetaName, offset, d.readQueueInfo.Offset())
		return ErrConfirmSizeInvalid
	}
	if offset == d.readQueueInfo.Offset() {
		if cnt == 0 {
			cnt = d.readQueueInfo.TotalMsgCnt()
		}
		if cnt != d.readQueueInfo.TotalMsgCnt() {
			nsqLog.LogErrorf("confirm read count invalid: %v:%v, %v", offset, cnt, d.readQueueInfo)
			return ErrConfirmCntInvalid
		}
	}
	if cnt == 0 && offset != BackendOffset(0) {
		nsqLog.LogErrorf("confirm read count invalid: %v:%v, %v", offset, cnt, d.readQueueInfo)
		return ErrConfirmCntInvalid
	}

	diffVirtual := offset - d.confirmedQueueInfo.Offset()
	newConfirm, err := stepOffset(d.dataPath, d.readFrom,
		d.confirmedQueueInfo, diffVirtual, d.readQueueInfo)
	if err != nil {
		nsqLog.LogErrorf("confirmed exceed the read pos: %v, %v", offset, d.readQueueInfo.Offset())
		return ErrConfirmSizeInvalid
	}
	if newConfirm.GreatThan(&d.queueEndInfo.EndOffset) || offset > d.queueEndInfo.Offset() {
		nsqLog.LogErrorf("confirmed exceed the end pos: %v, %v, %v", newConfirm, offset, d.queueEndInfo)
		return ErrConfirmSizeInvalid
	}
	d.confirmedQueueInfo.EndOffset = newConfirm
	d.confirmedQueueInfo.virtualEnd = offset
	atomic.StoreInt64(&d.confirmedQueueInfo.totalMsgCnt, cnt)
	d.updateDepth()
	//nsqLog.LogDebugf("confirmed to offset: %v:%v", offset, cnt)
	return nil
}

func (d *diskQueueReader) internalSkipTo(voffset BackendOffset, cnt int64, backToConfirmed bool) error {
	if voffset == d.readQueueInfo.Offset() {
		if cnt != 0 && d.readQueueInfo.TotalMsgCnt() != cnt {
			nsqLog.Logf("try sync the message count since the cnt is not matched: %v, %v", cnt, d.readQueueInfo)
			atomic.StoreInt64(&d.readQueueInfo.totalMsgCnt, cnt)
		}
		d.confirmedQueueInfo = d.readQueueInfo
		d.updateDepth()
		return nil
	}
	if voffset != d.readQueueInfo.Offset() && d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readBuffer.Reset()

	if voffset == d.confirmedQueueInfo.Offset() {
		if cnt != 0 && d.confirmedQueueInfo.TotalMsgCnt() != cnt {
			nsqLog.Logf("try sync the message count since the cnt is not matched: %v, %v", cnt, d.confirmedQueueInfo)
			atomic.StoreInt64(&d.confirmedQueueInfo.totalMsgCnt, cnt)
		}
		d.readQueueInfo = d.confirmedQueueInfo
		d.updateDepth()
		return nil
	}

	newPos := d.queueEndInfo.EndOffset
	var err error
	if voffset < d.confirmedQueueInfo.Offset() {
		nsqLog.Logf("skip backward to less than confirmed: %v, %v", voffset, d.confirmedQueueInfo.Offset())
		if !backToConfirmed {
			return ErrMoveOffsetInvalid
		}
	}

	if voffset > d.queueEndInfo.Offset() || cnt > d.queueEndInfo.TotalMsgCnt() {
		nsqLog.Logf("internal skip great than end : %v, skipping to : %v:%v", d.queueEndInfo, voffset, cnt)
		return ErrMoveOffsetOverflowed
	} else if voffset == d.queueEndInfo.Offset() {
		newPos = d.queueEndInfo.EndOffset
		if cnt == 0 {
			cnt = d.queueEndInfo.TotalMsgCnt()
		} else if cnt != d.queueEndInfo.TotalMsgCnt() {
			nsqLog.LogErrorf("internal skip count invalid: %v:%v, current end: %v", voffset, cnt, d.queueEndInfo)
			return ErrMoveOffsetInvalid
		}
	} else {
		if cnt == 0 && voffset != BackendOffset(0) {
			nsqLog.LogErrorf("confirm read count invalid: %v:%v, %v", voffset, cnt, d.readQueueInfo)
			return ErrMoveOffsetInvalid
		}

		newPos, err = stepOffset(d.dataPath, d.readFrom, d.readQueueInfo,
			voffset-d.readQueueInfo.Offset(), d.queueEndInfo)
		if err != nil {
			nsqLog.LogErrorf("internal skip error : %v, skipping to : %v", err, voffset)
			if os.IsNotExist(err) {
				nsqLog.Logf("internal skip because of not exist segment, try skip using the offset meta file")
				newPos = d.readQueueInfo.EndOffset
				for {
					if newPos.FileNum == d.queueEndInfo.EndOffset.FileNum {
						// we reach to the end segment of queue
						newPos.Pos = int64(voffset - (d.queueEndInfo.Offset() - BackendOffset(d.queueEndInfo.EndOffset.Pos)))
						if newPos.Pos < 0 {
							nsqLog.LogErrorf("skip error, current end: %v, skipto: %v, current: %v", d.queueEndInfo, voffset, newPos)
						} else {
							err = nil
						}
						break
					}
					// check offset meta
					_, metaStartPos, metaEndPos, innerErr := getQueueFileOffsetMeta(d.fileName(newPos.FileNum))
					if innerErr != nil {
						if os.IsNotExist(innerErr) {
							nsqLog.Logf("check segment offset meta not exist, try next: %v ", newPos)
							newPos.FileNum++
							newPos.Pos = 0
							continue
						}
						break
					}
					nsqLog.Logf("check segment: %v offset, %v, %v ", newPos, metaStartPos, metaEndPos)
					if voffset >= BackendOffset(metaEndPos) {
						newPos.FileNum++
						newPos.Pos = 0
					} else {
						newPos.Pos = int64(voffset - BackendOffset(metaStartPos))
						err = nil
						break
					}
				}
			}
			if err != nil {
				return err
			}
		}
	}

	if voffset < d.readQueueInfo.Offset() || nsqLog.Level() > levellogger.LOG_DEBUG {
		nsqLog.Logf("==== diskqueue(%s) read skip from %v to : %v, %v",
			d.readerMetaName, d.readQueueInfo, voffset, newPos)
	}
	d.readQueueInfo.EndOffset = newPos
	d.readQueueInfo.virtualEnd = voffset
	atomic.StoreInt64(&d.readQueueInfo.totalMsgCnt, cnt)
	if d.readQueueInfo.EndOffset.GreatThan(&d.queueEndInfo.EndOffset) {
		nsqLog.LogWarningf("==== read skip from %v to : %v, %v exceed end: %v", d.readQueueInfo,
			voffset, newPos, d.queueEndInfo)
		d.readQueueInfo = d.queueEndInfo
	}

	d.confirmedQueueInfo = d.readQueueInfo
	d.updateDepth()
	return nil
}

func (d *diskQueueReader) skipToNextFile() error {
	nsqLog.LogWarningf("diskqueue(%s) skip to next from %v, %v",
		d.readerMetaName, d.readQueueInfo, d.confirmedQueueInfo)
	if d.confirmedQueueInfo.EndOffset.FileNum >= d.queueEndInfo.EndOffset.FileNum {
		return d.skipToEndofQueue()
	}
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readBuffer.Reset()
	for {
		cnt, _, end, err := getQueueFileOffsetMeta(d.fileName(d.confirmedQueueInfo.EndOffset.FileNum))
		if err != nil {
			nsqLog.LogErrorf("diskqueue(%s) failed to skip to next %v : %v",
				d.readerMetaName, d.confirmedQueueInfo, err)
			if os.IsNotExist(err) && d.confirmedQueueInfo.EndOffset.FileNum < d.queueEndInfo.EndOffset.FileNum-1 {
				d.confirmedQueueInfo.EndOffset.FileNum++
				d.confirmedQueueInfo.EndOffset.Pos = 0
				continue
			}
			return err
		}
		d.confirmedQueueInfo.virtualEnd = BackendOffset(end)
		atomic.StoreInt64(&d.confirmedQueueInfo.totalMsgCnt, cnt)
		d.confirmedQueueInfo.EndOffset.FileNum++
		d.confirmedQueueInfo.EndOffset.Pos = 0
		break
	}
	if d.confirmedQueueInfo.EndOffset != d.readQueueInfo.EndOffset {
		nsqLog.LogErrorf("skip confirm to %v while read at: %v.", d.confirmedQueueInfo, d.readQueueInfo)
	}
	d.readQueueInfo = d.confirmedQueueInfo
	d.updateDepth()

	nsqLog.LogWarningf("diskqueue(%s) skip to next %v",
		d.readerMetaName, d.confirmedQueueInfo)
	return nil
}

func (d *diskQueueReader) skipToEndofQueue() error {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readBuffer.Reset()

	d.readQueueInfo = d.queueEndInfo
	if d.confirmedQueueInfo.EndOffset != d.readQueueInfo.EndOffset {
		nsqLog.LogErrorf("skip confirm from %v to %v.", d.confirmedQueueInfo, d.readQueueInfo)
	}
	d.confirmedQueueInfo = d.readQueueInfo
	d.updateDepth()

	return nil
}

func ensureReadBuffer(readFile *os.File, readBuffer *bytes.Buffer, dataNeed int64, curNum int64, currentRead int64, qend diskQueueEndInfo) (int64, error) {
	var n int64
	var err error
	if int64(readBuffer.Len()) < dataNeed {
		bufDataSize := dataNeed
		// at least we should buffer a buffer size
		if bufDataSize < int64(readBufferSize) {
			bufDataSize = int64(readBufferSize)
		}
		if curNum == qend.EndOffset.FileNum {
			// we should avoid prefetch uncommit file data after the committed queue end
			maxAllowSize := qend.EndOffset.Pos - currentRead
			if bufDataSize > maxAllowSize {
				bufDataSize = maxAllowSize
			}
		}
		n, err = io.CopyN(readBuffer, readFile, bufDataSize-int64(readBuffer.Len()))
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				nsqLog.LogErrorf("DISKQUEUE read to buffer error: %v (read), current read: %v, buffer(%v, %v), need: %v, err: %v, end: %v, readed: %v",
					n, currentRead, readBuffer.Len(), bufDataSize,
					dataNeed, err, qend, n)
				curPos, err2 := readFile.Seek(0, 1)
				nsqLog.Logf("seek to current: %v, %v", curPos, err2)
			}
		}
	}
	return n, err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueueReader) readOne() ReadResult {
	var result ReadResult
	d.readQueueInfo, d.readFile, result = diskReadOne(d.readerMetaName, d.readBuffer, d.fileName, d.readQueueInfo, d.queueEndInfo, d.readFile)
	return result
}

func diskReadOne(readerMetaName string, maybeBuffer *bytes.Buffer, fileName func(int64) string, readPos diskQueueEndInfo, endPos diskQueueEndInfo, readFile *os.File) (diskQueueEndInfo, *os.File, ReadResult) {
	var result ReadResult
	var msgSize int32
	result.Offset = BackendOffset(0)
	if readPos.totalMsgCnt <= 0 && readPos.Offset() > 0 {
		result.Err = ErrReadQueueCountMissing
		nsqLog.Warningf("diskqueue(%v) read offset invalid: %v (this may happen while upgrade, wait to fix)", readerMetaName, readPos)
		return readPos, readFile, result
	}
	if readPos == endPos {
		result.Err = io.EOF
		return readPos, readFile, result
	}
CheckFileOpen:

	result.Offset = readPos.Offset()
	if readFile == nil {
		curFileName := fileName(readPos.EndOffset.FileNum)
		readFile, result.Err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if result.Err != nil {
			if readPos.Offset() == endPos.Offset() && readPos.EndOffset == endPos.EndOffset {
				if os.IsNotExist(result.Err) {
					result.Err = io.EOF
				}
			}
			return readPos, readFile, result
		}

		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.Debugf("DISKQUEUE readOne() opened %s", curFileName)
		}

		if readPos.EndOffset.Pos > 0 {
			_, result.Err = readFile.Seek(readPos.EndOffset.Pos, 0)
			if result.Err != nil {
				nsqLog.LogWarningf("DISKQUEUE %s: seek error %s", curFileName, result.Err)
				tmpStat, tmpErr := readFile.Stat()
				if tmpErr != nil {
					nsqLog.LogWarningf("DISKQUEUE %s: stat error %s", curFileName, tmpErr)
				} else {
					nsqLog.LogWarningf("DISKQUEUE %s: stat %v", curFileName, tmpStat)
				}
				readFile.Close()
				readFile = nil
				return readPos, nil, result
			}
		}
	}

	defer func() {
		if result.Err != nil {
			if maybeBuffer != nil {
				maybeBuffer.Reset()
			}
			readFile.Close()
			readFile = nil
		}
	}()

	if readPos.EndOffset.FileNum > endPos.EndOffset.FileNum {
		nsqLog.LogWarningf("DISKQUEUE(%s): read %v exceed current end %v", readerMetaName,
			readPos, endPos)
		result.Err = errors.New("exceed end of queue")
		return readPos, nil, result
	}

	var dreader io.Reader
	dreader = readFile
	if maybeBuffer != nil {
		var rn int64
		dreader = maybeBuffer
		rn, result.Err = ensureReadBuffer(readFile, maybeBuffer, 4, readPos.EndOffset.FileNum, readPos.EndOffset.Pos, endPos)
		if result.Err != nil {
			if result.Err == io.EOF {
				if maybeBuffer.Len() >= 4 {
					// do consume buffer since we already have read to buffer
				} else if maybeBuffer.Len() == 0 {
					if readPos.EndOffset.FileNum < endPos.EndOffset.FileNum {
						readFile, readPos = handleReachEnd(readFile, maybeBuffer, readPos)
						nsqLog.Logf("DISKQUEUE(%s): readOne() read end, try next: %v",
							readerMetaName, readPos.EndOffset.FileNum)
						goto CheckFileOpen
					}
					nsqLog.LogWarningf("DISKQUEUE(%s): read %v error %v, end: %v", readerMetaName, readPos, result.Err, endPos)
					return readPos, nil, result
				} else {
					return readPos, nil, result
				}
			} else {
				nsqLog.LogWarningf("DISKQUEUE(%s): ensure buffer error: %v, %v", readerMetaName, result.Err.Error(), rn)
				return readPos, nil, result
			}
		}
	}
	result.Err = binary.Read(dreader, binary.BigEndian, &msgSize)
	if result.Err != nil {
		nsqLog.LogWarningf("DISKQUEUE(%s): read at %v (end %v) error %v", readerMetaName, readPos, endPos, result.Err)
		if result.Err == io.EOF && readPos.EndOffset.FileNum < endPos.EndOffset.FileNum {
			readFile, readPos = handleReachEnd(readFile, maybeBuffer, readPos)
			goto CheckFileOpen
		}
		tmpStat, tmpErr := readFile.Stat()
		if tmpErr != nil {
			nsqLog.LogWarningf("DISKQUEUE(%s): stat error %s", readerMetaName, tmpErr)
		} else {
			nsqLog.LogWarningf("DISKQUEUE(%s): stat %v", readerMetaName, tmpStat)
		}
		return readPos, nil, result
	}

	if msgSize <= 0 || msgSize > MAX_POSSIBLE_MSG_SIZE {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		result.Err = fmt.Errorf("invalid message read size (%d) at offset: %v", msgSize, readPos)
		return readPos, nil, result
	}

	result.Data = make([]byte, msgSize)
	if maybeBuffer != nil {
		var rn int64
		rn, result.Err = ensureReadBuffer(readFile, maybeBuffer, int64(msgSize), readPos.EndOffset.FileNum, readPos.EndOffset.Pos+4, endPos)
		if result.Err != nil {
			if result.Err == io.EOF && maybeBuffer.Len() >= int(msgSize) {
				//
			} else {
				tmpStat, _ := readFile.Stat()
				nsqLog.LogWarningf("DISKQUEUE(%s): ensure buffer for msg body error %v, %v, left %v, need: %v, stats: %v",
					readerMetaName, result.Err.Error(), rn, maybeBuffer.Len(), msgSize, tmpStat)
				return readPos, nil, result
			}
		}
	}
	_, result.Err = io.ReadFull(dreader, result.Data)
	if result.Err != nil {
		nsqLog.LogWarningf("DISKQUEUE(%s): read %v error %v, %v", readerMetaName, readPos, result.Err, msgSize)
		tmpStat, tmpErr := readFile.Stat()
		if tmpErr != nil {
			nsqLog.LogWarningf("DISKQUEUE(%s): stat error %s", readerMetaName, tmpErr)
		} else {
			nsqLog.LogWarningf("DISKQUEUE(%s): stat %v", readerMetaName, tmpStat)
		}
		return readPos, nil, result
	}

	result.Offset = readPos.Offset()

	totalBytes := int64(4 + msgSize)
	result.MovedSize = BackendOffset(totalBytes)
	oldCnt := readPos.TotalMsgCnt()
	oldPos := readPos.EndOffset

	result.CurCnt = atomic.AddInt64(&readPos.totalMsgCnt, 1)
	readPos.EndOffset.Pos = readPos.EndOffset.Pos + totalBytes
	readPos.virtualEnd += BackendOffset(totalBytes)

	if readPos.virtualEnd == endPos.virtualEnd {
		if readPos.totalMsgCnt != 0 && readPos.TotalMsgCnt() != endPos.TotalMsgCnt() {
			nsqLog.LogWarningf("disk %s: message read count not match with end: %v, %v", readerMetaName, readPos, endPos)
		}
		readPos.totalMsgCnt = endPos.totalMsgCnt
	}

	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("=== read move forward: %v:%v to %v", oldPos, oldCnt,
			readPos)
	}

	if readPos.EndOffset.GreatThan(&endPos.EndOffset) {
		nsqLog.LogWarningf("read exceed end: %v, %v", readPos, endPos)
	}
	return readPos, readFile, result
}

func handleReachEnd(readFile *os.File, readBuffer *bytes.Buffer, readPos diskQueueEndInfo) (*os.File, diskQueueEndInfo) {
	if readFile != nil {
		readFile.Close()
		readFile = nil
	}
	if readBuffer != nil {
		readBuffer.Reset()
	}
	readPos.EndOffset.FileNum++
	readPos.EndOffset.Pos = 0
	return readFile, readPos
}

func (d *diskQueueReader) syncAll(fsync bool) error {
	d.needSync = false
	return d.persistMetaData(fsync, d.confirmedQueueInfo, d.queueEndInfo)
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueueReader) retrieveMetaData() error {
	// since the old meta data is not compatible with new, we use a new file for new version meta.
	// if no new version meta, we need read from old and generate new version file.
	fileNameV2 := d.metaDataFileName(true)
	c, qe, errV2 := d.metaStorage.RetrieveReader(fileNameV2)
	if errV2 == nil {
		d.confirmedQueueInfo = c
		d.queueEndInfo = qe
	} else {
		nsqLog.Infof("new meta file err : %v", errV2)

		fileName := d.metaDataFileName(false)
		c, qe, err := d.metaStorage.RetrieveReader(fileName)
		if err != nil {
			return err
		}
		d.confirmedQueueInfo = c
		d.queueEndInfo = qe

		if d.confirmedQueueInfo.virtualEnd == d.queueEndInfo.virtualEnd {
			d.confirmedQueueInfo.totalMsgCnt = d.queueEndInfo.totalMsgCnt
		}

		d.persistMetaData(false, d.confirmedQueueInfo, d.queueEndInfo)
	}
	if d.confirmedQueueInfo.TotalMsgCnt() == 0 && d.confirmedQueueInfo.Offset() != BackendOffset(0) {
		nsqLog.Warningf("reader (%v) count is missing, need fix: %v", d.readerMetaName, d.confirmedQueueInfo)
		// the message count info for confirmed will be handled by coordinator.
		if d.confirmedQueueInfo.Offset() == d.queueEndInfo.Offset() {
			d.confirmedQueueInfo.totalMsgCnt = d.queueEndInfo.totalMsgCnt
		}
	} else if d.confirmedQueueInfo.Offset() == d.queueEndInfo.Offset() &&
		d.confirmedQueueInfo.TotalMsgCnt() != d.queueEndInfo.TotalMsgCnt() {
		nsqLog.Warningf("the reader (%v) meta count is not matched with end: %v, %v",
			d.readerMetaName, d.confirmedQueueInfo, d.queueEndInfo)
		d.confirmedQueueInfo = d.queueEndInfo
	}
	d.readQueueInfo = d.confirmedQueueInfo
	d.updateDepth()

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueReader) persistMetaData(fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error {
	fileName := d.metaDataFileName(true)
	return d.metaStorage.PersistReader(fileName, fsync, confirmed, queueEndInfo)
}

func (d *diskQueueReader) metaDataFileName(newVer bool) string {
	if newVer {
		return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.v2.reader.dat"),
			d.readerMetaName)
	}
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.reader.dat"),
		d.readerMetaName)
}

func GetQueueFileName(dataRoot string, base string, fileNum int64) string {
	if fileNum > int64(FileNumV2Seq) {
		return fmt.Sprintf(path.Join(dataRoot, "%s.diskqueue.%09d.dat"), base, fileNum)
	}
	return fmt.Sprintf(path.Join(dataRoot, "%s.diskqueue.%06d.dat"), base, fileNum)
}

func (d *diskQueueReader) fileName(fileNum int64) string {
	return GetQueueFileName(d.dataPath, d.readFrom, fileNum)
}

func (d *diskQueueReader) checkTailCorruption() {
	if d.readQueueInfo.EndOffset.FileNum < d.queueEndInfo.EndOffset.FileNum || d.readQueueInfo.EndOffset.Pos < d.queueEndInfo.EndOffset.Pos {
		return
	}

	// we reach file end, the readQueueInfo.EndOffset should be exactly at the end of file.
	if d.readQueueInfo.EndOffset != d.queueEndInfo.EndOffset {
		nsqLog.LogErrorf(
			"diskqueue(%s) read to end at readQueueInfo.EndOffset != endPos (%v > %v), corruption, skipping to end ...",
			d.readerMetaName, d.readQueueInfo, d.queueEndInfo)
		d.skipToEndofQueue()
		d.needSync = true
	}
}

func (d *diskQueueReader) handleReadError() {
	// should not change the bad file, just log it.
	err := d.skipToNextFile()
	if err != nil {
		return
	}
	nsqLog.LogWarningf("diskqueue(%s) skip error to next %v",
		d.readerMetaName, d.readQueueInfo)
	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

func (d *diskQueueReader) internalUpdateEnd(endPos *diskQueueEndInfo, forceReload bool) (bool, error) {
	if endPos == nil {
		return false, nil
	}
	if forceReload {
		nsqLog.Logf("read force reload at end %v ", endPos)
	}

	if endPos.Offset() == d.queueEndInfo.Offset() && endPos.TotalMsgCnt() == d.queueEndInfo.TotalMsgCnt() {
		return false, nil
	}
	d.needSync = true
	if d.readQueueInfo.EndOffset.GreatThan(&endPos.EndOffset) || d.readQueueInfo.Offset() > endPos.Offset() {
		if d.readQueueInfo.Offset() <= endPos.Offset() {
			nsqLog.Logf("%v new end old than the read end: %v, %v, %v",
				d.readerMetaName, d.readQueueInfo,
				endPos, d.queueEndInfo)
			// reader for file position is great than end but the virtual offset is smaller,
			// something is wrong. (the file position should be fixed using the virtual offset)
			// The confirmed offset should be fixed together.
			// it may be cleaned since too old, we can just reset to the start keeped of the queue
			queueStart, err := getQueueStart(d.dataPath, d.readFrom)
			if err != nil {
				return false, err
			}
			nsqLog.LogWarningf("%v fix read and confirmed file position: %v, %v to start %v",
				d.readerMetaName, d.readQueueInfo, d.confirmedQueueInfo,
				queueStart)

			if queueStart.Offset() > endPos.Offset() {
				queueStart = *endPos
			}
			d.readQueueInfo = queueStart
			d.confirmedQueueInfo = queueStart
			forceReload = true
		} else {
			if !forceReload {
				// if rollback or reset, should set the force reload flag
				nsqLog.Logf("%v new end old than the read end: %v, %v, %v",
					d.readerMetaName, d.readQueueInfo.EndOffset,
					endPos, d.queueEndInfo)
				return false, nil
			}
			d.readQueueInfo = *endPos
			forceReload = true
		}
	}
	if d.confirmedQueueInfo.EndOffset.GreatThan(&d.readQueueInfo.EndOffset) ||
		d.confirmedQueueInfo.Offset() > d.readQueueInfo.Offset() {
		d.confirmedQueueInfo = d.readQueueInfo
	}

	if endPos.Offset() > d.confirmedQueueInfo.Offset() {
		atomic.StoreInt32(&d.waitingMoreData, 0)
	}
	oldPos := d.queueEndInfo
	d.queueEndInfo = *endPos
	d.updateDepth()
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("read end %v updated to : %v, current confirmed: %v ", oldPos, endPos, d.confirmedQueueInfo)
	}
	if forceReload {
		nsqLog.LogDebugf("read force reload at end %v ", endPos)
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		d.readBuffer.Reset()
	}

	return true, nil
}
