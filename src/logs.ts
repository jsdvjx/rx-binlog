import { existsSync, mkdirSync, readdirSync, writeFileSync, readFileSync, readFile, createReadStream, open, statSync, watch } from "fs"
import { Observable, Observer } from "rxjs"
interface Position {
    log: string;
    size: number;
}
interface Read { log: string, size: number, init: boolean }
export class Logs {
    private static path = process.cwd() + '/logs'
    private static start_index = 1000000
    private getNewPath = () => {
        if (!existsSync(Logs.path)) {
            mkdirSync(Logs.path)
        }
        return `${Logs.path}/${this.files.reduce((res, acc) => {
            const i = parseInt(acc.split('.').shift() || Logs.start_index.toString())
            return res > i ? res : i
        }, Logs.start_index) + 1}.log`
    }
    private log: string = this.getNewPath();
    private index = Logs.path + '/index'
    private readidx = Logs.path + '/read'
    private size: number = 0;
    private get files() {
        return readdirSync(Logs.path).filter(i => /^\d+\.log$/.test(i))
    }
    constructor() {
        writeFileSync(Logs.path + '/index', JSON.stringify({ log: this.log, size: -1 }))
    }
    write = (chunk: Uint8Array) => {
        this.size += chunk.length;
        if (this.size > 1024 * 1024 * 100) {
            writeFileSync(Logs.path + '/index', JSON.stringify({ log: this.log, size: this.size }))
            this.log = this.getNewPath();
            this.size = 0;
        }
        writeFileSync(this.log, chunk, { flag: 'a' })
    }
    private reading = false;
    read = () => {
        if (this.reading) {
            return null;
        }
        this.reading = true;
        const readPosition = this.getLastLog()
        return new Observable<string>((obser: Observer<string>) => {
            const stream = createReadStream(readPosition.log, { start: readPosition.init ? 0 : readPosition.size });
            stream.on('data', (chunk) => {
                obser.next(chunk)
            })
            stream.on('close', () => {
                readPosition.size += stream.bytesRead;
                readPosition.init = false;
                writeFileSync(this.readidx, JSON.stringify(readPosition))
                this.reading = false;
                obser.complete();
            })
        })
    }
    getLastLog = (): Read => {
        const start_str = Logs.start_index.toString();
        if (existsSync(this.readidx)) {
            const read = JSON.parse(readFileSync(this.readidx).toString()) as Read
            const write = JSON.parse(readFileSync(this.index).toString()) as Position
            if (read.log === write.log) {
                return read;
            } else {
                if (statSync(read.log).size !== read.size) {
                    return read;
                } else {
                    const id = (parseInt(read.log.split('/').pop() || (start_str + '.log').split('.').shift() || start_str)) + 1;
                    if (existsSync(`${Logs.path}/${id}.log`)) {
                        return { log: `${Logs.path}/${id}.log`, size: 0, init: false }
                    }
                    return read;
                }
            }
        }
        const log = `${Logs.path}/${this.files.sort((a, b) => parseInt(a.split('.').shift() || start_str) - parseInt(b.split('.').shift() || start_str)).shift() || (start_str + '.log')}`
        return { log, size: 0, init: true }
    }
}
