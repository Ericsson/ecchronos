# ECChronos tool

Standalone ecChronos includes a commandline utlity (ecctool) to check status, start/stop ecChronos service as well as trigger a single repair.

## Subcommands

The following subcommands are available:

| Command               | Description                              |
|-----------------------|------------------------------------------|
| `ecctool repairs`     | Repair status overview                   |
| `ecctool schedules`   | Status of schedules                      |
| `ecctool run-repair`  | Trigger a single repair                  |
| `ecctool repair-info` | Show information about repairs per table |
| `ecctool start`       | Start ecChronos service                  |
| `ecctool stop`        | Stop ecChronos service                   |
| `ecctool status`      | Show status of ecChronos service         |

### Flags

Each of the subcommands support flags. For more information provide `-h` flag after subcommand.
For example:

```
ecctool repairs -h
```