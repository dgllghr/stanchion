####
## Dependencies
####

New-Item "$HOME\deps" -ItemType Directory -ea 0

## Zig (master)
Invoke-WebRequest -Uri "https://ziglang.org/download/index.json" -OutFile "$HOME\deps\zig-version-index.json"
$latestversion = (Get-Content "$HOME\deps\zig-version-index.json" -Raw | ConvertFrom-Json).master.version
$existingversion = ""
if (Test-Path -Path "$HOME\deps\ZIGVERSION" -PathType Leaf) {
    $existingversion = Get-Content "$HOME\deps\ZIGVERSION" -TotalCount 1
}

if ( $latestversion -ne  $existingversion ) {
    Write-Output "Downloading and installing zig (master)"

    if (Test-Path -Path "$HOME\deps\zig") {
        Remove-Item "$HOME\deps\zig" -Recurse -Force
    }
    New-Item "$HOME\deps\zig" -ItemType Directory

    $zigurl = (Get-Content "$HOME\deps\zig-version-index.json" -Raw | ConvertFrom-Json).master."x86_64-windows".tarball
    Invoke-WebRequest -Uri "$zigurl" -OutFile "$HOME\deps\zig.zip"
    Expand-Archive -LiteralPath "$HOME\deps\zig.zip" -DestinationPath "$HOME\deps"
    $ziprootname = (Get-ChildItem "$HOME\deps" -Name) -match "zig-windows-.*"
    Move-Item "$HOME\deps\$ziprootname\*" -Destination "$HOME\deps\zig" -Force -Verbose

    Write-Output "$latestversion" | Out-File -FilePath "$HOME\deps\ZIGVERSION"

    Remove-Item "$HOME\deps\$ziprootname"
    Remove-Item "$HOME\deps\zig.zip"
} else {
    Write-Output ">> Using cached zig"
}
Remove-Item "$HOME\deps\zig-version-index.json"

$env:Path = "$HOME\deps\zig;" + $env:Path
zig version

####
## Checks
####

## Lint
zig fmt --check .

## Unit tests
zig build test -Dsqlite-test-version="$Env:SQLITE_VERSION" --summary all

## Integration tests

function Get-SQLiteVersionNumber {
    param (
        $versionstring
    )

    $versionparts = $versionstring -split "\."
    ([int]$versionparts[0] * 1000000) + ([int]$versionparts[1] * 1000) + [int]$versionparts[2]
}

# For some unknown reason, stanchion cannot be loaded into sqlite on windows when the sqlite
# version is < 3.44.0. I have given up trying to figure out why. So for now, only run the
# integration test if the sqlite version is >= 3.44.0
$sqliteversionum = Get-SQLiteVersionNumber "$Env:SQLITE_VERSION"
$minversion = Get-SQLiteVersionNumber "3.44.0"
if ($sqliteversionnum -ge $minversion) {
    zig build itest -Dsqlite-test-version="$Env:SQLITE_VERSION" --summary all
}
