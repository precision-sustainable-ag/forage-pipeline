# Step 1:
`import_kobo_forage.R`
 - Reads in forms from the Kobo server
 - pulls out the CSVs and images
 - renames them and uploads to blob
 - generates list of errors to review with Step 2
 - TODO update container name
 
# Step 2:
`forage_review.R`
 - Look at errors
 - Decide which can't be fixed, add those IDs to the top of `import_kobo_forage.R`
 - If error can be fixed, #TODO
 
# Step 3:
`blob_to_plot_pipeline.R`
 - Reads in scan txt files inside blob storage
 - Parses into proper format
 - Splits into Onfarm, CE1, CE2 groups
 - Identifies: 
    - loops (onfarm), 
    - tip-ups between plots within rep (CE1), #TODO does this extract strictly the 4 longest segments?
    - tip-ups between plots in same-rep column of field block (CE2, #TODO)
 - Labels plots with proper IDs:
   - Onfarm: built in to filename
   - CE1: built in to filename and column `plot_id`
   - CE2:built in to filename and column `plot_id`
 - Upload labeled files to blob storage in next container, #TODO

# Step 4:
`labeled_to_datum.R`
 - Read in labeled scan files from blob storage #TODO
 - Creates datum
 - Converts LIDAR and SONAR to centimeters above ground
 - Upload calibrated files to blob storage in next container, #TODO

# Step 5:
`datum_to_summary_bm.R`
 - Generate regression input metrics for each plot (proportions, means, SD, etc)
 - #TODO read in biomass from each source and join to summary
   - Onfarm: partial
   - CE1: TODO
   - CE2: TODO
 - Upload calibrated files to blob storage in next container, #TODO
