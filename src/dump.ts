import { BinlogOption } from "./binlog";
import { spawn, exec } from "child_process";
import { Observable, Observer } from "rxjs";
import { writeFileSync } from "fs";

export class Dump {
    private path = `${__dirname}/../bin/mysqldump`
    constructor(private option: BinlogOption) { }
    private get dbParam() {
        return [
            `--host=${this.option.host}`,
            `--password=${this.option.password}`,
            `--user=${this.option.username}`
        ]
    }
    stream = (option: { src: { table: string, database: string }, dest: string }) => {
        return new Observable<string>((obser: Observer<string>) => {
            const stream = exec(`${this.path} ${this.dbParam.join(' ')} --single-transaction -q --databases ${option.src.database} --tables ${option.src.table}`, { maxBuffer: 1024 * 1024 * 1024  });
            let last: string = ''
            stream.stdout?.on('data', (chunk: string) => {
                const tmp = (last + chunk).split('\n').filter(i => i.indexOf('INSERT INTO') === 0 && i.length > 10);
                last = (tmp.pop() || '')
                for (const sql of tmp) {
                    obser.next(sql.replace(`\`${option.src.table}\``, option.dest).replace(/\\'/g, `''`))
                }
            })
            stream.stdout?.on('end', () => {
                obser.next(last.replace(`\`${option.src.table}\``, option.dest).replace(/\\'/g, `''`))
            })
            stream.stderr?.on('data', (chunk) => {
            })
            stream.on('error', (error) => {
                obser.error(error);
            })
            stream.on('close', () => {
                obser.complete()
            })
        })


    }

}