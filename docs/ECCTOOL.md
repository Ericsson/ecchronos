# ECChronos tool

Standalone ecChronos includes a commandline utlity (ecctool) to check repair status, repair config, start/stop ecChronos service as well as trigger a single repair.

## Subcommands

The following subcommands are available:

| Command                                                             | Description                           |
|---------------------------------------------------------------------|---------------------------------------|
| `ecctool repair-status`                                             | Repair status overview                |
| `ecctool repair-config`                                             | Configuration overview                |
| `ecctool trigger-repair`                                            | Trigger a single repair               |
| `ecctool start`                                                     | Start ecChronos service               |
| `ecctool stop`                                                      | Stop ecChronos service                |
| `ecctool status`                                                    | Show status of ecChronos service      |

### Flags

Each of the subcommands support flags. For more information provide `-h` flag after subcommand.
For example:

```
ecctool repair-status -h
```