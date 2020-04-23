# glueetl
A command line tool to help deploy AWS Glue Jobs at ease :)

## Install
```bash
pip install glueetl
```

## Initialize a Glue job
```bash
mkdir sample
cd sample
glueetl init
```
```
.
├── README.md
├── config.yaml
└── script.py
```

Please change default values in file `config.yaml` and write your job logic in file `script.py`.

## Deploy a Glue job
```bash
cd sample
glueetl deploy
```

## Run a Glue job
```bash
cd sample
glueetl run --arg1=value1 --arg2=value2
```
