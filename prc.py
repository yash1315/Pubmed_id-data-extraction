#!usr/bin/env bash

import pandas as pd
import numpy as np
import json
import os
import warnings
warnings.filterwarnings("ignore")

def read_json(path):
    f = [json_ for json_ in os.listdir(path) if json_.endswith('.json')]
    return f

def pdf_json_file(path):
    # path_pdf = "X:\LAB\document_parses\pdf_json_200"
    print("PDF Running.....")
    path_pdf = path
    pdf_title =[]
    pdf_location_settlement =[]
    # pdf_location_region=[]
    pdf_location_country =[]
    pdf_paper_id =[]
    body=[]
    temp_body =[]
    files_ = read_json(path_pdf)
    for index_, js_ in enumerate(files_):
        with open(os.path.join(path_pdf, js_)) as json_file_:
            json_pdf = json.load(json_file_)

            # Body Text
            for indx in range(len(json_pdf["body_text"])):
                temp_body.append(json_pdf["body_text"][indx]["text"])
            body.append(temp_body)
            temp_body=[]

            # Title and Paper_id/Sha
            try:
                pdf_title.append(json_pdf["metadata"]["title"])
                pdf_paper_id.append(json_pdf["paper_id"])
            except:
                pdf_title.append("Null")
                pdf_paper_id.append("Null")

            if json_pdf["metadata"].get("authors"):
                if json_pdf["metadata"]["authors"][0].get("affiliation"):
                    if json_pdf["metadata"]["authors"][0]["affiliation"].get("location"):
                        try:
                            # Extracting Country
                            pdf_location_country.append(json_pdf["metadata"]["authors"][0]["affiliation"]["location"]["country"])   #Location
                        except KeyError:
                            pdf_location_country.append("Null")
                    else:
                        pdf_location_country.append("Null")

                    if "settlement" in json_pdf["metadata"]["authors"][0]["affiliation"].keys():
                        pdf_location_settlement.append(json_pdf["metadata"]["authors"][0]   ["affiliation"]["location"]["settlement"])
                    else:
                        pdf_location_settlement.append("Null")
                else:
                    pdf_location_country.append("Null")
                    pdf_location_settlement.append("Null")
            else:
                pdf_location_country.append("Null")
                pdf_location_settlement.append("Null")

    return pdf_title,pdf_location_country,pdf_paper_id,body

def pdf_dataframe(pdf_title,pdf_location_country,pdf_paper_id,body):

    # Creating the dataframe
    pdf_json_df = pd.DataFrame(columns=["sha","title","Body","country"])
    pdf_json_df["sha"] = pdf_paper_id
    pdf_json_df["title"] = pdf_title
    pdf_json_df["country"]= pdf_location_country
    pdf_json_df["Body"] = body

    # Pre-processing the countries name
    India = ["India, Sweden"]
    Japan = ['Japan (']
    China = ["People's Republic of China","China'"]
    Germany = ['Germany., Germany., Germany']
    Canada = ['Canada, Argentina, India, Pakistan, China']

    """replace_countries = {
    "India, Sweden": "India",
    'Japan (' : "Japan",
    "People's Republic of China,China'" : "China",
    'Germany., Germany., Germany' : "Germany",
    'Canada, Argentina, India, Pakistan, China' : "Canada"
    }
    pdf_json_df = pdf_json_df.assign("country"=pdf_json_df["country"].replace   (replace_countries))"""

    #replacing the country names
    pdf_json_df["country"].replace(Japan,"Japan",inplace=True)
    pdf_json_df["country"].replace(China,"China",inplace=True)
    pdf_json_df["country"].replace(Germany,"Germany",inplace=True)
    pdf_json_df["country"].replace(Canada,"Canada",inplace=True)
    pdf_json_df["country"].replace(India,"India",inplace=True)

    return pdf_json_df

def pubtator(path):
    print("Pubtator_running......")
    f = read_json(path)
    for ind, j in enumerate(f):
        with open(os.path.join(path, j)) as js:
            jp = json.load(js)

    pmid=[]
    pmcid = []
    authors = []
    anno = []

    for jp_index in range(len(jp[1])):
        pmid.append(jp[1][jp_index]["_id"].split("|")[0]) # pmid
        pmcid.append(jp[1][jp_index]["pmcid"]) #pmcid
        authors.append(jp[1][jp_index]["authors"])   #authors
        temp_anno = {"Species" : [],"Gene":[],"Disease":[],"Chemical":[],"Mutation":[],"CellLine":[]}
        for ps_index in range(len(jp[1][jp_index]["passages"])):
            try:
                for an_index in range(len(jp[1][jp_index]["passages"][ps_index]["annotations"])):
                    type_ =jp[1][jp_index]["passages"][ps_index]["annotations"][an_index]["infons"]["type"] #annotation
                    temp_anno[type_].append(jp[1][jp_index]["passages"][ps_index]["annotations"][an_index]["text"])
                    #df[f"{type}"] = df[f"{type}"].append(f"{jp[1][jp_index]["passages"][ps_index]["annotations"][an_index]["text"]}")
            except (KeyError):
                continue
        anno.append(temp_anno)


    return pmid,pmcid,authors,anno

def pubatator_dataframe(pmid,pmcid,authors,anno):

    df = pd.DataFrame(columns=["pubmed_id","pmcid","authors","annotations","Gene","Species","Disease","Chemical","Mutation","CellLine"])
    #df.rename(columns={"pmid":"pubmed_id"},inplace=True)
    df["authors"] = authors
    df["annotations"] = anno
    df["pubmed_id"] = pmid
    df["pmcid"] = pmcid
    df.Species = pd.DataFrame.from_dict(anno)["Species"]
    df.Gene = pd.DataFrame.from_dict(anno)["Gene"]
    df.Disease = pd.DataFrame.from_dict(anno)["Disease"]
    df.Chemical = pd.DataFrame.from_dict(anno)["Chemical"]
    df.Mutation = pd.DataFrame.from_dict(anno)["Mutation"]
    df.CellLine = pd.DataFrame.from_dict(anno)["CellLine"]

    return df

def read_csv_data(metadata_path):
    # "X:\LAB\metadata.csv\metadata.csv"
    metadata = pd.read_csv(metadata_path).fillna("0")
    metadata.drop(metadata[metadata.pubmed_id.str.startswith("x.doi.org/",na=False)].index,inplace=True)
    metadata.pubmed_id = np.array(metadata.pubmed_id).astype("Int64").astype("str")
    metadata.pmcid =  np.array(metadata.pmcid).astype("str")
    metadata.drop(metadata[(metadata.pubmed_id == "0") & (metadata.pmcid == "0")].index,axis=0,inplace=True)

    # assert metadata[(metadata.pubmed_id == "0") & (metadata.pmcid == "0")]

    metadata.drop(columns=["mag_id","who_covidence_id","arxiv_id","license","pdf_json_files","pmc_json_files","url","s2_id"],axis=1,inplace=True)

    return metadata
def main():
    # path_pdf = input("PDF_Json : ")
    # path_pubtator = input("Pubtator path : ")
    # meta_data_path = input("Metadata path : ")
    server_path = os.getcwd()

    ##########################################################

    path_pdf = server_path + "/pdf_json"
    title, location, id, body = pdf_json_file(path_pdf)
    df_pdf = pdf_dataframe(title, location, id, body)
    df_pdf.to_csv("pdf_json.csv")
    print("Extracted PDF_JSON File .......... 100%")

    ##########################################################

    path_pubtator =server_path
    pmid,pmcid,authors,anno = pubtator(path_pubtator)
    df_pubtator = pubatator_dataframe(pmid,pmcid,authors,anno)
    df_pubtator.to_csv("pubtator.csv")
    print("Extracted Pubtator Json File .......... 100%")

    ###########################################################

    meta_data_path= server_path +  "/metadata.csv"
    meta_data = read_csv_data(meta_data_path)
    print("Metadata File ....... 100%")

    ###########################################################

    #Merging data
    merge_data = meta_data.merge(df_pdf,on="sha",how="left").merge(df_pubtator,on="pubmed_id",how="left")
   # merge_data = meta_data.merge(df_pubtator,on="pubmed_id",how="left")
    merge_data.to_csv("Final_output.csv")
    print("Data Extracted")
    print("Done")


if __name__ == "__main__":
    main()
