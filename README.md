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
  ------------
  
