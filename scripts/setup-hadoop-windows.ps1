param(
    [string]$HadoopHome = 'C:\hadoop',
    [switch]$Persist
)

function Write-Info {
    Write-Host "[INFO] $args" -ForegroundColor Cyan
}

function Write-WarningText {
    Write-Host "[WARNING] $args" -ForegroundColor Yellow
}

function Write-ErrorText {
    Write-Host "[ERROR] $args" -ForegroundColor Red
}

Write-Info "Validando instalação do Hadoop nativo para Windows..."

if (-not (Test-Path $HadoopHome)) {
    Write-ErrorText "O diretório HADOOP_HOME não existe: $HadoopHome"
    Write-ErrorText "Crie o diretório e copie os binários do Hadoop Windows para lá."
    exit 1
}

$binPath = Join-Path $HadoopHome 'bin'
if (-not (Test-Path $binPath)) {
    Write-ErrorText "O diretório bin não existe: $binPath"
    exit 1
}

$winutils = Join-Path $binPath 'winutils.exe'
$hadoopDll = Join-Path $binPath 'hadoop.dll'

if (-not (Test-Path $winutils)) {
    Write-WarningText "winutils.exe não encontrado em $binPath"
} else {
    Write-Info "winutils.exe encontrado."
}

if (-not (Test-Path $hadoopDll)) {
    Write-WarningText "hadoop.dll não encontrado em $binPath"
} else {
    Write-Info "hadoop.dll encontrado."
}

Write-Info "Definindo HADOOP_HOME e atualizando PATH para a sessão atual..."
$env:HADOOP_HOME = $HadoopHome
$env:PATH = "$binPath;$env:PATH"

Write-Info "HADOOP_HOME configurado para: $env:HADOOP_HOME"
Write-Info "PATH atualizado para incluir: $binPath"

if ($Persist) {
    Write-Info "Persistindo HADOOP_HOME e PATH no perfil do usuário..."
    [Environment]::SetEnvironmentVariable('HADOOP_HOME', $HadoopHome, 'User')
    [Environment]::SetEnvironmentVariable('PATH', "$env:PATH", 'User')
    Write-Info "Variáveis persistidas no perfil do usuário. Abra um novo terminal para aplicar."
} else {
    Write-Host "`nPara persistir as variáveis no Windows, abra um terminal com elevação e execute:`n" -ForegroundColor Green
    Write-Host "setx HADOOP_HOME \"$HadoopHome\""
    Write-Host "setx PATH \"$($env:PATH)\""
}

Write-Host "`nDepois, abra um novo terminal e verifique com:`n" -ForegroundColor Green
Write-Host "echo $env:HADOOP_HOME"
Write-Host "where.exe winutils.exe"
