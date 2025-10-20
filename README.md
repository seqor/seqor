<p align=center>
  <img src="logo.png" />
</p>

Seqor is a cost-effective, Loki compatible database for logs.

### Build
The build will automatically resolve dependencies listed in `build.zig.zon`.
```bash
zig build
```
This produces the `Seqor` executable in `zig-out/bin/`.

### Run
By default Seqor looks for `seqor.yaml` in the current directory.
```bash
zig build run
```
Specify a custom config file:

TODO: For some reason this doesn't work.
```bash
zig build run -- -c ./my-seqor.yaml
```

### Configuration
Example `seqor.yaml`:
```yaml
server:
  port: 9012
app:
  maxRequestSize: 4194304 # 4MB
```

## Dependency Resources
###### Where to look for zig dependencies
1. https://zigistry.dev/
2. https://ziglist.org/

