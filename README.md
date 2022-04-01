# Transponster

![Transponster](https://media2.giphy.com/media/QRPx31XOyvCpO/giphy.gif?cid=ecf05e47pvs0fgsj34qz3lfwitqy4oqlkcnm2t84ppk7rwb8&rid=giphy.gif&ct=g)

## Usage

```txt
usage: transponster [-h] -i INPUT_COLLECTION -o OUTPUT_COLLECTION -s SCRIPT [--scratch_location SCRATCH_LOCATION] [-n MAX_ITEMS_PER_STAGE]

Execute a script on files stored in iRODS. The script must take as input a folder, and place its ouput in a folder named 'output' which will be created for it.

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_COLLECTION, --input_collection INPUT_COLLECTION
  -o OUTPUT_COLLECTION, --output_collection OUTPUT_COLLECTION
  -s SCRIPT, --script SCRIPT
  --scratch_location SCRATCH_LOCATION
  -n MAX_ITEMS_PER_STAGE, --max_items_per_stage MAX_ITEMS_PER_STAGE
```
