# RECV.R4X

`RECV.R4X` is an independent R4OS application implemented in Zig.

## Package

- Version: `0.1.2`
- Image target: `/R4OS/SOFTWARE/TERMINAL/RECV.R4X`
- Image scope: `full`
- Canonical project manifest: `module.R4MF`

The manifest is the single source of truth for the artifact, imports, image
target, and package metadata.

## Build

On Windows:

    Build.bat

On Linux or macOS:

    ./Build.sh

The build starters resolve the current local R4OS dependency checkouts through
`Settings.R4S`. The URL and hash entries in `build.zig.zon` record the
last verified standalone dependency identities; workspace builds use the
mapped local checkouts.

## Documentation

Detailed German technical notes from the migration are preserved in
`DOCUMENTATION.de.txt`. Source-transfer provenance is recorded in
`PROVENANCE.txt`.

## License

Original R4OS material is licensed under Apache License 2.0. See `LICENSE`
and `NOTICE`. Any repository-specific external material is documented in
`THIRD_PARTY_NOTICES.md`.


RECV 0.1.2 receives into a create-only sibling stream with a unique
8.3 name. It retains the existing target until the complete body, checksum and
stream flush/finish succeed, then uses the existing atomic replacement facade.
The stage and backup names are checked against the target using the backend's
name collation. Abort only cleans the stream it owns; no receive error deletes
the destination.

/RESUME copies and checksums the existing prefix into the private stage,
requests only the missing suffix and publishes after checking the whole file.
A failed attempt keeps the original prefix; a retry starts from that same
confirmed prefix. /NOOVERWRITE rejects ordinary replacement even when /RESUME
is enabled. Explicit resumed transfers may complete an existing partial file;
a missing target is still published with the create-only ownership contract.
An empty resumed transfer also creates its missing destination.

The backup is removed only after confirmed publication. An ambiguous storage
error during publication leaves the stage/backup names visible in the error
message, retaining recovery material rather than deleting it. Existing
filesystem publication/recovery guarantees apply; this is not a new journal.
Decimal size/checksum fields are checked for overflow before arithmetic.

RECV opens its own listener before Accept and closes it on every return.
The waiting message is emitted after successful listener admission.
