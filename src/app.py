from utils import *
import pandas as pd
import json

def link_drugs_to_journals(df,source,drugslist) -> pd.DataFrame:
    """Return a dataframe, each row is a drug and a paper which quotes the drug """
    res=[]
    #For each row of df, extract drugs mentionned in the title
    for index, row in df.iterrows():
        mentionned_drugs=extract_word_from_sentence(row["title"],drugslist["drug"])
    #Link each drug mentionned with its paper
    for drug in mentionned_drugs:
        catalog=res.append([drug,row["title"],row["date"],row["journal"],f"{source}"])
    df=pd.DataFrame(res,columns=["drug","title","date","journal","source"])
    return df


def final_results(df):
    """Groupby df to generat final catalog : [{drug_name:name1, drug_id=id1,publications:[{pub1...},{pub2...}]}]"""
    final=[]
    for name,group in df.groupby(by=["drug","atccode"],as_index=True):
        drug_catalog={}
        journal_catalog=[]
    for row_index,row in group.iterrows():
        drug_catalog["drug_name"]=name[0]
        drug_catalog["drug_id"]=name[1]
        journal_catalog.append(row.to_dict())
        drug_catalog["publications"]=journal_catalog
        final.append(drug_catalog)
    return final

def write_final_results(final):
    final_json=json.dumps(final)
    with open('./data/output/drugs_catalog.json',"w") as f:
        f.write(final_json)
    

def extract():
    """Return input data as Pandas DataFrames"""
    drugs=csv_to_df("./data/input/drugs.csv")
    pub=csv_to_df("./data/input/pubmed.csv")
    trials=csv_to_df("./data/input/clinical_trials.csv")
    return drugs,pub,trials

def transform():
    """Clean,transform and return our final catalog"""
    drugs,pub,trials=extract()[0],extract()[1],extract()[2]
    #rename columns trials data 
    trials=trials.rename(columns={"scientific_title":"title"})

    #remove rows with nan values 
    trials=trials.dropna()
    pub=pub.dropna()

    #link a drugs to journals for each data source
    pub_catalog=link_drugs_to_journals(pub,"PubMed",drugs)
    trials_catalog=link_drugs_to_journals(trials,"ClinialTrials",drugs)

    #concat our two dataframe to have only one dataframe
    catalog=pd.concat([pub_catalog,trials_catalog])

    #left join to get drugs_id
    catalog=catalog.merge(drugs,on="drug",how='left',sort=True)

    #Generate and return our final catalog 
    final_catalog=final_results(catalog)
    return final_catalog



def load():
    """ Dump final_catalog and write it to the output directory """
    final_catalog=transform()
    write_final_results(final_catalog)
    


if __name__=="__main__":
    extract()
    transform()
    load()

