# new versions of venv will be in "\di.medusa\SamaritanHouse\Python_Venv" 


# variables : update the paths according to the environemnt 

	# python project root folder
	Set-Variable -Name "project_root" -Value "C:\Users\srikanth.marella\Documents\GitHub\Snowflake\_export_to_csv_files"

	# base python installation
	Set-Variable -Name "py_base_path" -Value "C:\Python\Python38\python.exe"       

	# is install requirements?   options 0/1
	Set-Variable -Name "is_install_requirements" -Value 1


#*****************************************************************************************************************************#

# variables: do not need update    

	# venv for project, on cnc box
	Set-Variable -Name "venv_folder" -Value "$project_root\venv"             
	Set-Variable -Name "req_file_path" -Value "$project_root\requirements.txt"                   
    
	# python.exe path on venv
	Set-Variable -Name "venv_path" -Value "$venv_folder\Scripts\python.exe"
	
	# pip.exe path on venv
	Set-Variable -Name "venv_pip_path" -Value "$venv_folder\Scripts\pip.exe"


# print statements 

	Write-Output " " 
	Write-Output "Paths from variables : " 
	Write-Output "		project_root  : $project_root    "
	Write-Output "		py_base_path  : $py_base_path    "
	Write-Output "		venv_folder   : $venv_folder     "
	Write-Output "		venv_path     : $venv_path       "
	Write-Output "		req_file_path : $req_file_path   "
	Write-Output " " 


# Check if the venv folder exists
if(
    !(Test-Path -Path $venv_folder )
  )
    {
		# Create new folder
        New-Item -ItemType directory -Path $venv_folder
        Write-Output "New folder created"
		Write-Output " "

		# Python creating new venv
        & $py_base_path -m venv $venv_folder
        Write-Output "created virtual environment under..$py_base_path"
		Write-Output " "
		
		# Upgrading pip to latest
        & $venv_path -m pip install --upgrade pip
		Write-Output "pip upgraded"
		Write-Output " "

        # Install pipdeptree to get the dependencies
        & $venv_path -m pip install pipdeptree
        Write-Output "Installed pipdeptree"
        Write-Output " "
		
        # Check if to install requirements ?
		if ( $is_install_requirements -eq 0 )
			{
				Write-Output "is_install_requirements is 0. Skipping installing requirements"
				Write-Output " "
			}
		else
		{
			Write-Output "installing packages from requirements.txt . . . "
			Write-Output " "
			
			& $venv_pip_path install -r $req_file_path
			
			Write-Output "created packages from requirements.txt . . . "
			Write-Output " "
		}


    }
else
# if venv folder exists, skip creating venv
{
    Write-Output $venv_folder" : venv folder already exists. Manually delete it venv folder and re run the powershell script"
}



