# Transponster

![Transponster](https://media2.giphy.com/media/QRPx31XOyvCpO/giphy.gif?cid=ecf05e47pvs0fgsj34qz3lfwitqy4oqlkcnm2t84ppk7rwb8&rid=giphy.gif&ct=g)

Transponster will download the next files to process and upload the previous files generated whilst doing a batch of processing: it allows processing to be done with both minimal staging area and minimal blocking on downloading input and uploading output.

## Requirements

Transponster depends on [partisan](https://github.com/wtsi-npg/partisan) and as such requires the `baton-do` executable from [baton](https://github.com/wtsi-npg/baton) to be installed.

## Usage

```txt
usage: transponster [-h] -i INPUT_COLLECTION -o OUTPUT_COLLECTION -s SCRIPT [--scratch_location SCRATCH_LOCATION] [-n MAX_ITEMS_PER_STAGE] [-p | --progress_bar | --no-progress_bar] [-v | --verbose | --no-verbose]

Execute a script on files stored in iRODS. The script must take as input a folder, and place its ouput in a folder named 'output' which will be created for it.

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_COLLECTION, --input_collection INPUT_COLLECTION
  -o OUTPUT_COLLECTION, --output_collection OUTPUT_COLLECTION
  -s SCRIPT, --script SCRIPT
  --scratch_location SCRATCH_LOCATION
  -n MAX_ITEMS_PER_STAGE, --max_items_per_stage MAX_ITEMS_PER_STAGE
  -p, --progress_bar, --no-progress_bar
  -v, --verbose, --no-verbose
```
