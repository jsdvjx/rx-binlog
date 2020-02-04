import { Subject } from "rxjs";

export class BinlogResolver {
    private status: 'INIT' = 'INIT'
    public data = new Subject<string>()
}