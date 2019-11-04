# tidb-v3.0.0-wrapped

a wrapped tidb wrapped by [tidb-wrapper](https://github.com/fuzzdebugplatform/tidb-wrapper),  in TiDB 2019 hackthon

1. open trace

```
:43222/switch
```

2. execute a sql in tidb

3. find the sql digest

```
:43222/status
```

4. look trace

```
:43222/trace/248be6a1
```

> `248be6a1` is digest of the sql you want to look
