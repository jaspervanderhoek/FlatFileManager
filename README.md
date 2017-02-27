# Fixed Length Exporter

## Description
This module can provide a solution to be able to export fixed length text files without having to program any line of code. Configure the data mapping similar to the db replication and excel importer module, create a microflow which calls the export action and you'll be able to download the ascii file without any problems.


## Typical usage scenario
This module is best use for Flat file exports with fixed length or when using separators (csv). When configuring the export you have the ability to setup a 'Template Set'. This set of templates allows you generate a single file that has multiple formats of data in it. In finance and health care it is common to communicate with ASCII files that have different record types. 

HEADORDER12346589My Order Description   20160320
LINE001ForProduct1        3000
LINE002ForProduct2        3000
FOOTER0000300006000

The above file can be exported using a single TemplateSet containing multiple sub-templates. The module will export all the data for each of the templates in the specified order. 
 
## Features and limitations
    - Create your own data mapping
    - Export your data with prefixes or suffix
    - Format the values with a microflow before export it
    - Re-use mappings and templates

## Configuration
After importing the module, you should connect the “FixedLengthExporter.ExportSet_Overview” form to your application navigation. This is the starting place for defining the export mappings. Create a new export set, an export set is the combination of all the different lines. The ExportSet object start at one object and you'll be able to create different export lines for all the different lines or associations.