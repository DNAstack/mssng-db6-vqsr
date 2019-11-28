# MSSNG DB6 VQSR Pipeline

Perform VQSR on the `GVCFtyper_main` files - they will be combined into a single file, VQSR performed, and then re-split by chromosome. All chromosome `GVCFtyper_main` files should be input here, and one `GVCFtyper_main.recal` recalibrated main file per chromosome will be output. Also input the `chr__.bed` region files from the previous step.
