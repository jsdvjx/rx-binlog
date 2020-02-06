import { Subject, of, Observable, Observer, concat, from, timer, zip, interval } from "rxjs";
import { readFileSync } from "fs";
import moment from "moment";
import { tap, map, mergeMap, filter, switchMap, concatMap } from "rxjs/operators";
export enum EVENT_TYPE {
    FORMAT_DESCRIPTION_EVENT = 1,
    GTID_LOG_EVENT,
    PREVIOUS_GTIDS_EVENT,
    QUERY_EVENT,
    TABLE_MAP_EVENT,
    WRITE_ROW_EVENT,
    UPDATE_ROW_EVENT,
    DELETE_ROW_EVENT,
    XID_EVENT,
    ROTATE_EVENT,
}
export type EventSimpleInfo = {
    emit_at: Date,
    end_log_pos: number,
    server_id: number
}
export type EventHead = {
    emit_at: Date,
    end_log_pos: number,
    server_id: number,
    type: EVENT_TYPE,
    qtype?: 'UPDATE' | 'INSERT' | 'DELETE',
    ext: any,
    file: string,
    head: string,
}
type StringProp<T> = {
    [P in keyof T]: string;
}
type TypeChecker = {
    [k in EVENT_TYPE]: RegExp;
};

type STATUS = 'EVENT_START' | 'EVENT_READING' | 'EVENT_END';
export class BinlogResolver {
    private status: STATUS = 'EVENT_END'
    private position: number = 0;
    //private static start_regexp = /^#(?<emit_at>\d{6}\s+[\d:]{5,8}) server id (?<server_id>\d+)\s+end_log_pos (?<end_log_pos>\d+).+?$/m
    private static start_regexp = /^#(?<emit_at>\d{6}\s+[\d:]{5,8}) server id (?<server_id>\d+)\s+end_log_pos (?<end_log_pos>\d+).+?(Delete_row|Write_row|Update_row|Rotate|Table_map).+?$/m
    private static end_regexp = /^# at \d+$/m
    private static end_content_regexp = /[\w\W]*?# at \d+/
    private head_str = '';
    private event_str = '';
    private file = '';
    push = (str: string) => {
        if (this.status !== 'EVENT_READING') {
            this.event_str += str
        }
        else {
            const match = BinlogResolver.end_content_regexp.exec(str);
            if (match) {
                this.event_str = str.slice(match[0].length)
                this.reading$.next(match[0]);
                this.reading$.complete()
                this.eventEnd()
            } else {
                this.reading$.next(str.toString())
            }
        }
    }
    constructor() {
        //this.event_str = readFileSync('./1.txt').toString()
    }

    private read = () => {
        const info = this.readHead();
        if (info) {
            const result = this.readToEventEnd(info);
            if (result) {
                return result.pipe(map(str => [str, info] as [string, EventHead]))
            }
        }
        return of(null)
    }
    stream = () => {
        return interval(0).pipe(concatMap(() => this.read().pipe(
            //@ts-ignore
            filter(i => i !== null)
        ))) as Observable<[string, EventHead]>
    }
    private readHead = () => {
        if (this.status !== 'EVENT_END') {
            return null
        }
        const match = BinlogResolver.start_regexp.exec(this.event_str);
        if (match) {
            this.setStatus('EVENT_START')
            this.event_str = this.event_str.slice(match.index)
            this.head_str = match[0]
            this.event_str = this.event_str.slice(this.head_str.length);
            const _info = BinlogResolver.getEsi(match.groups as any)
            const info = BinlogResolver.headResolve(this.head_str, _info);
            if (info?.type === EVENT_TYPE.ROTATE_EVENT) {
                this.file = info.ext.file || ''
            }
            if (info === null) {
                this.eventEnd()
            }
            if (info) info.file = this.file;
            return info
        }
        return null
    }
    private setStatus = (status: STATUS) => {
        this.status = status
    }
    private static getEsi = (info: StringProp<EventSimpleInfo>) => {
        return ({
            emit_at: moment('20' + info.emit_at, 'YYYYMMDD HH:mm:ss').toDate(),
            server_id: parseInt(info.server_id),
            end_log_pos: parseInt(info.end_log_pos)
        }) as EventSimpleInfo
    }
    private reading$ = new Subject<string>()
    private eventEnd = () => {
        this.head_str = '';
        this.setStatus('EVENT_END')
    }
    readToEventEnd = (info: EventHead) => {
        const cut = /[\w\W]+?(?=### UPDATE|### DELETE|### INSERT)/;
        const mul = /^### (UPDATE|DELETE|INSERT)[\w\W]+?(?=### UPDATE|### DELETE|### INSERT|# at)/mg
        const endReg = /[\w\W]+(?=### UPDATE|### DELETE|### INSERT)/;
        if (this.status === 'EVENT_END') {
            throw new Error('event was not started')
        }
        const end = BinlogResolver.end_regexp.test(this.event_str)
        let result: Observable<string> | null = null;
        if (end && this.status === 'EVENT_START') {
            const match = BinlogResolver.end_content_regexp.exec(this.event_str);
            if (match) {
                this.event_str = this.event_str.slice(match.index);
                this.event_str = this.event_str.slice(match[0].length)
                this.eventEnd()
                if ([EVENT_TYPE.UPDATE_ROW_EVENT, EVENT_TYPE.WRITE_ROW_EVENT, EVENT_TYPE.DELETE_ROW_EVENT].includes(info.type)) {
                    result = from(Array.from(match[0].match(mul) || []))
                } else {
                    result = of(match[0])
                }
            }
        } else {
            this.setStatus('EVENT_READING');
            const tmp = this.event_str;
            this.event_str = '';
            delete this.reading$;
            this.reading$ = new Subject();
            result = concat(of(tmp), this.reading$)
        }
        let last = ''
        if (!result) return null
        return result.pipe(
            mergeMap(i => {
                if ([EVENT_TYPE.UPDATE_ROW_EVENT, EVENT_TYPE.WRITE_ROW_EVENT, EVENT_TYPE.DELETE_ROW_EVENT].includes(info.type) && this.status === 'EVENT_READING') {
                    i += last
                    const n = i.match(cut);
                    if (n) {
                        i = i.slice(n.index)
                    }
                    const _e = i.match(endReg);
                    if (_e) {
                        last = i.slice(_e[0].length)
                        i = _e[0]
                    }
                    return from(Array.from(i.match(mul) || []))
                }
                return of(i);
            })
        )
    }
    private static headResolve = (head: string, info: EventSimpleInfo) => {
        const checker: Partial<TypeChecker> = {};
        //checker[EVENT_TYPE.FORMAT_DESCRIPTION_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Start: (?<description>.+)$/m;
        //checker[EVENT_TYPE.GTID_LOG_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?GTID	last_committed=(?<last_committed>\d+)	sequence_number=(?<sequence_number>\d+)	rbr_only=(?<rbr_only>.+?)\s*.*?$/m;
        //checker[EVENT_TYPE.PREVIOUS_GTIDS_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Previous-GTIDs$/m;
        checker[EVENT_TYPE.TABLE_MAP_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Table_map: (?<table>.+?) mapped to number (?<map_to>\d+)$/m
        checker[EVENT_TYPE.WRITE_ROW_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Write_rows.*?: table id (?<table_id>\d+) flags: STMT_END_F$/m
        checker[EVENT_TYPE.UPDATE_ROW_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Update_rows.*?: table id (?<table_id>\d+) flags: STMT_END_F$/m
        checker[EVENT_TYPE.DELETE_ROW_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Delete_rows.*?: table id (?<table_id>\d+) flags: STMT_END_F$/m
        //checker[EVENT_TYPE.XID_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Xid = (?<xid>\d+)$/m
        checker[EVENT_TYPE.ROTATE_EVENT] = /Rotate to (?<file>mysql-bin\.\d+?)  pos: (?<pos>\d+?)$/m
        //checker[EVENT_TYPE.QUERY_EVENT] = /CRC32 0x(?<crc32>[a-f\d]{8})\s+?Query	thread_id=(?<thread_id>\d+?)	exec_time=(?<exec_time>\d+?)	error_code=(?<error_code>\d+)$/m
        for (const [type, regexp] of Object.entries(checker as TypeChecker)) {
            const result = regexp.exec(head);
            let qtype = '';
            if (result) {
                const ext: Record<string, any> = { ...result.groups };
                switch (parseInt(type) as any as EVENT_TYPE) {
                    case EVENT_TYPE.DELETE_ROW_EVENT:
                    case EVENT_TYPE.WRITE_ROW_EVENT:
                    case EVENT_TYPE.UPDATE_ROW_EVENT:
                        if (EVENT_TYPE.DELETE_ROW_EVENT === parseInt(type)) qtype = 'DELETE';
                        if (EVENT_TYPE.WRITE_ROW_EVENT === parseInt(type)) qtype = 'INSERT';
                        if (EVENT_TYPE.UPDATE_ROW_EVENT === parseInt(type)) qtype = 'UPDATE';
                        ext.table_id = parseInt(ext.table_id)
                        break;
                    case EVENT_TYPE.FORMAT_DESCRIPTION_EVENT:
                    case EVENT_TYPE.PREVIOUS_GTIDS_EVENT:
                        break;
                    case EVENT_TYPE.GTID_LOG_EVENT:
                        ext.last_committed = parseInt(ext.last_committed)
                        ext.sequence_number = parseInt(ext.sequence_number)
                        break;
                    case EVENT_TYPE.TABLE_MAP_EVENT:
                        ext.map_to = parseInt(ext.map_to)
                        break;
                    case EVENT_TYPE.XID_EVENT:
                        ext.xid = parseInt(ext.xid)
                        break;
                    case EVENT_TYPE.QUERY_EVENT:
                        ext.thread_id = parseInt(ext.thread_id)
                        ext.exec_time = parseInt(ext.exec_time)
                        ext.error_code = parseInt(ext.error_code)
                        break;
                    default:
                        break;
                }
                return { ...info, qtype, head, type: parseInt(type), ext } as any as EventHead
            }
        }
        return null
    }
}