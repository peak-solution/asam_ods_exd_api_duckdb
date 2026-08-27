# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.9.2]

### Fixed

- `ExternalDataFile.get_values` no longer raises `Invalid channel id` for channels
  written by `ValuesAppenderService.Append` after the channel metadata cache was
  already built (e.g. following a prior `GetStructure`/`GetValues` call on the same
  handle). The cache is now refreshed on demand when a requested channel id is
  missing, instead of only becoming visible after the file handle is evicted by
  the idle-connection auto-close.
- DT_DATE values are returned in ODS format instead of ISO


## [0.9.1] - 2026-07-23

### Fixed

- Split insert dataframe by datatype

## [0.9.0] - 2026-07-22

### Added

- Initial release