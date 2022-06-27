# JSONJoinerCrunch

Implemented the code for joining three JSON files on the basis of their keys using Crunch.
* First, the files were read from the input path.
* The file was split on the basis of new line
* The key and values were extracted from each line and emitted.
* Using collectValues(), the key adnvalues were aggregated into a JSON object and then were finally written into the output file.
