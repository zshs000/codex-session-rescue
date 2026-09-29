$ErrorActionPreference = 'Stop'
$appRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$pythonConsole = (& python -c 'import sys; print(sys.executable)').Trim()
$pythonWindowless = Join-Path (Split-Path -Parent $pythonConsole) 'pythonw.exe'
if (!(Test-Path -LiteralPath $pythonWindowless -PathType Leaf)) { throw 'pythonw.exe was not found.' }
$desktopDir = [Environment]::GetFolderPath('Desktop')
$linkPath = Join-Path $desktopDir 'Codex 会话修复助手.lnk'
$linkShell = New-Object -ComObject WScript.Shell
$shortcut = $linkShell.CreateShortcut($linkPath)
if ((Test-Path -LiteralPath $linkPath) -and $shortcut.Arguments -and !$shortcut.Arguments.Contains($appRoot)) {
    $linkPath = Join-Path $desktopDir ('Codex 会话修复助手-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.lnk')
    $shortcut = $linkShell.CreateShortcut($linkPath)
}
$shortcut.TargetPath = $pythonWindowless
$shortcut.Arguments = '"' + (Join-Path $appRoot 'app.py') + '" --open'
$shortcut.WorkingDirectory = $appRoot
$shortcut.IconLocation = (Join-Path $appRoot 'web\icon.ico') + ',0'
$shortcut.Description = '搜索 Codex 会话，检查子代理与分段历史，备份清理和回滚。'
$shortcut.Save()
[PSCustomObject]@{ Shortcut = $linkPath; Python = $pythonWindowless; App = (Join-Path $appRoot 'app.py') } | ConvertTo-Json
