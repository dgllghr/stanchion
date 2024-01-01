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
zig build test --summary all

# TODO run integration test on Windows once the integration test runner supports Windows