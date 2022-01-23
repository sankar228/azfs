# azfs
## This utility is Used to access Azure blob and execute key operations easily on linux and windows
`
Note: windows version is still evaluation phase, some of the functionality may not work as expected
`

---
### set the necessary environment variables
```shell
export AZ_STORAGE_ACCOUNT_KEY=xxxxxxxxxxxxxx
export AZ_STORAGE_ACCOUNT_NAME=xxxxxxxxxxxxx
export AZ_STORAGE_CONTAINER=xxxxxxxxxxxxxxxx

```
## Usage, Help gives complete details on how we can use this utility
```shell
azfs -h
```
`
usage[ ./azfs  <options> 
-h | --help : Help !! 
-c | --copy : Copy/Dowload file from the blob storage eg: -c <blob path> [local dirctory] default to current dir 
-u | --upload : Upload a file to blob storage eg: -u <bloblocation> <localfile1> <localfile2> ...
-l | --list : List directories or files a given blob/container eg: -l <blob path> 
-d | --delete : Delete a file/blob from the blob storage -d <blob path> 
-md | --makedir : Create folder/dir in a given blob path eg: -md <blob path> <dir Name>
`

#### Functionality is going to actively added ...