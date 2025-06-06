# Scan-Pipeline
Pipeline to Categorize - Upload pdfs via Google Drive 
the tool parameters :
1. takes csv file that's contains the names of the PDFs on the server
2. Source path : the paent path of the server i.e when we need to take specific claims from forest of enormous paths of folders hence the tool design with gready search to find out the padf
3. credential file : the credential.json that we must to pull out from Google Cloud Console (for Google drive api)
4. Parent Folder Id : the id of the folder tht we needs to put the sub folder (the sub foler where the folder we will put the member onformation file within it)
5. sub-folder : takes string (the name of the folder that the tool will through the pdfs within it)
6. advanced options :
     * dpi
     * quality
     * coloured/greyscale (coloured if we needs soft copy exact the same for original hard copy including "Seals and signatures")
     * max-workers : the cores which e will specify to working in the process of ETL
     * API rate limit : the rate of transmition process
       ![image](https://github.com/user-attachments/assets/42d56a0f-922c-4827-b833-df65d5fe6573)

  ------------
  the workflow for the ETL tool :
  1. after it takes the csv it will search for the scanned files on th server using gready search
  2. after fin it will creates TEMP foler on the user partition (in random name)
  3. the tool will resize the files size to enhance the speed of Uploaing by the specified quality by the user 
  4. the tool will connect to the google rive using api by provided credential json file provided by the user
  5. the tool designed with conditions to reload the process in case of failure 
i.e if we needs to upload about 10000 file with 11mb after resizing with good quality nearly about 3mb per file i.e we haveto upload,
 about 30 GB at end of day hence to avoid any failure causing stopping the process it designed to not to stop
