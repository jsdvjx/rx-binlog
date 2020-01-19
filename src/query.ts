import { Connection } from 'mysql2';
import { Observable, Observer } from 'rxjs';
export class Query {
    constructor(private conn: Connection) { }
    run = <T = any>(sql: string) => {
        return new Observable<T>((obser: Observer<T>) => {
            const strem = this.conn.query(sql);
            strem.on('result', (row) => {
                obser.next(row as any)
            })
            strem.on('error', obser.error.bind(obser));
            strem.on('end', obser.complete.bind(obser))
        })
    }
}