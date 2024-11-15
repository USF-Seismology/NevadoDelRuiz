# NevadoDelRuiz
A repository for sharing codes for processing Nevado Del Ruiz data and related research

Run codes in this order:

* 00_gcf2sds.py:               Converts the continuous GCF archive (2-minute files) into day-long MiniSEED files in SDS directory tree/filenaming convention
* 10_create_RSAM.ipynb:        Computes RSAM metrics (including frequency metrics) for each minute of data, for each net.sta.loc.chan using SAM.py module. Saved into one CSV file per net.sta.loc.chan per year.
* 20_plot_RSAM_metrics.ipynb:  Loads and plots RSAM metrics, e.g. fratio (frequency ratio)

Glenn Thompson, 2024/11/15
