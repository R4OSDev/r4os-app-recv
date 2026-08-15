RECV.R4X
========

RECV.R4X ist ein Terminalwerkzeug fuer einfache Netzwerk-Empfangstests ueber
R4NET.

Projektstruktur seit 0.51.19:
- `build.zig` baut die App als eigenes SDK-Projekt.
- `build.zig.zon` bindet `r4os_sdk` als Paket.
- `module.R4MF` beschreibt Artefakt, Zielpfad, Imports und Contract.

Build:

    cd Code\System\Software\Recv
    ..\..\..\DevTools\Zig\zig.exe build

Ergebnis:

    Code\System\Software\Recv\zig-out\RECV.R4X

Contract:
- R4XStart-Entry: `recv_main`
- App-Klasse: `console`
- R4L-Imports: `R4SYS`, `R4NET`
- Zielpfad im Image: `C:\R4OS\SOFTWARE\TERMINAL\RECV.R4X`

