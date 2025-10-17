<p align=center>
  <img src="logo.png" />
</p>

Seqor is a cost-effective, Loki compatible database for logs.

## Installation

### Prerequisites
- Zig (master or latest stable). Check with `zig version`.
- Git (for version embedding and cloning).

### Clone the Repository
```bash
git clone git@github.com:seqor/seqor.git
cd seqor
```

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
(The `--` separates Zig build arguments from application CLI args.)

### Configuration
Example `seqor.yaml`:
```yaml
server:
  port: 9012
app:
  maxRequestSize: 4194304 # 4MB
```

### Testing
Run all unit tests:
```bash
zig build test
```
Filter tests by substring (example):
```bash
zig build test -Dtest-filter="SIGTERM"
```

### Upgrading
Pull latest changes and rebuild:
```bash
git pull
zig build
```
Version string comes from the current git tag or `<branch>-<short-sha>`.
Tag a release to embed a clean version:
```bash
git tag v0.1.0
zig build
```

## Dependency Resources
###### Where to look for zig dependencies
1. https://zigistry.dev/
2. https://ziglist.org/

