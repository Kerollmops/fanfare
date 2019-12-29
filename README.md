# fanfare

A simple timeseries database based on LMDB.
This highly inspired by [njaard/sonnerie](https://github.com/njaard/sonnerie).

## Installation

```bash
cargo install --git https://github.com/Kerollmops/fanfare.git
```

## Usage

You must create the database directory first.

```bash
mkdir -p mytimeseries
```

Once done, you can write some data in fanfare.

```bash
echo "\
oceanic-airlines 2001-01-13T12:09:14.026490 ff 37.686751 -122.602227
oceanic-airlines 2001-01-13T12:09:14.026500 ff 37.686751 -122.602227
oceanic-airlines 2001-01-13T12:09:14.026501 ff 37.686751 -122.602227
oceanic-airlines 2001-01-13T12:09:14.026502 ff 37.686751 -122.602227"\
 | fanfare write -d mytimeseries
```

And read it back, you can also filter it using the [glob syntax](https://en.wikipedia.org/wiki/Glob_(programming)#Syntax).

```bash
fanfare read -d mytimeseries --filter 'ocean?c-airlin[!a-d]s' -d mytimeseries
```
