Unicode true
ManifestDPIAware true
!define APPNAME "gk.ImporterTools"
!define APPPATH "Indusoft.Alcor.DataServer"
!define COMPANYNAME "InduSoft"
!define DESCRIPTION "gk.ImporterTools"
!define LIMSTOOLSPATH "c:\"
!define BUILDSPATH "C:\LIMS_Resources\Builds"
RequestExecutionLevel admin ;Require admin rights on NT6+ (When UAC is turned on)
ShowInstDetails show
InstallDir "$PROGRAMFILES\${COMPANYNAME}\${APPNAME}"
Name "${COMPANYNAME} - ${APPNAME}"
outFile "Indusoft.Alcor.DataServer.LIMSpackage.exe"
 
!include LogicLib.nsh
 
# Just three pages - license agreement, install location, and installation
# page license
Page directory
Page instfiles
 
!macro VerifyUserIsAdmin
UserInfo::GetAccountType
pop $0
${If} $0 != "admin" ;Require admin rights on NT4+
        messageBox mb_iconstop "Administrator rights required!"
        setErrorLevel 740 ;ERROR_ELEVATION_REQUIRED
        quit
${EndIf}
!macroend
 
function .onInit
	setShellVarContext all
	!insertmacro VerifyUserIsAdmin
functionEnd
 
section "install"
	setOutPath $INSTDIR	
	#nsExec::Exec 'net stop InduSoft.Alcor.DataServer'
	File /r D:\work\i-lds-imp-exp\i-lds-imp-exp\bin\Debug\*.*  
	#nsExec::Exec 'net start InduSoft.Alcor.DataServer'
sectionEnd