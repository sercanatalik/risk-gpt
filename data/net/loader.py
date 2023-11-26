import pandas as pd 
import glob
import os  # noqa: E401
path = os.getcwd()

print(path)
data_files = glob.glob(os.path.join(path,"data/net/*.csv"))
print(data_files)
df = pd.concat((pd.read_csv(file) for file in data_files), ignore_index=True)

df = df.rename(columns={"Unnamed: 0": "Company"})
df = df.set_index("Company")

df = df.groupby(by=df.index).first()
print(df.head(1000))



df.to_csv(os.path.join(path,"data/net/finroberta_net_combined.csv"), index=True)