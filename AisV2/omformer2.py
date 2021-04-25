import pandas as pd
import os

string = str
typeAisDK={'Time stamp':string,'Class':string,'MMSI':int,'Latitude':string,"Longitude":string,"Nav Status":string,"ROT":str,"SOG":str,"COG":str,"Heading":str,"IMO number":str,"Call sign":string,"Ship name":string, "Ship type":string,"Cargo":string,"C":str,'A':str,'Signal type':string,'Maximum actual draught':str,'Destination':string,'Expected time':string,'Ais':string}
typeAisNO={'MMSI':int,'Ship name':string,'Call sign':string,'IMO number':str,'Ship type':string,'Destination':string,'Length':str,'Breadth':str,'Maximum actual draught':string,'Cargo':string,'Latitude':string,'Longitude':str,'Time stamp':string,'SOG':string,'COG':str,'Heading':str,'Nav Status':string}
typeDMA={"MMSI":int,"Ship name":string,"Call sign":string,"IMO number":str,"Ship type":string,"Destination":string,"Maximum actual draught":str,"Cargo":string,"Latitude":str,"Longitude":str,"Unix time":int,"SOG":str,"COG":string,"Heading":string,"Nav Status":string,"ROT":string,"A":string,"B":string,"C":str,"D":string,"Time stamp":string,"Empty":object}

def AisDK(filepath):
	df=pd.read_csv(filepath, sep=";", header=None, names=['Time stamp','Class','MMSI','Latitude',"Longitude","Nav Status","ROT","SOG","COG","Heading","IMO number","Call sign","Ship name", "Ship type","Cargo","C",'A','Signal type','Maximum actual draught','Destination','Expected time','Ais'], dtype=typeAisDK)
	df2=df[['MMSI','Ship name','Call sign','IMO number','Ship type','Destination','Maximum actual draught','Cargo','Latitude', 'Longitude']]
	df2['Unix time']=df2.shape[0]*pd.np.nan
	df2['SOG']=df['SOG']
	df2['COG']=df['COG']
	df2['Heading']=df['Heading']
	df2['Nav status']=df['Nav Status']
	df2['ROT']=df['ROT']
	df2['A']=df['A']
	df2['B']=df2.shape[0]*pd.np.nan
	df2['C']=df['C']
	df2['D']=df2.shape[0]*pd.np.nan
	df2['Time stamp']=df['Time stamp']
	df2['Expected time']=df['Expected time']
	df2['Signal type']=df['Signal type']
	df2['Ais']=df['Ais']
	return df2

def AisNO(filepath):
	df=pd.read_csv(filepath, sep = "\t", header=None, names=['MMSI','Ship name','Call sign','IMO number','Ship type','Destination','Length','Breadth','Maximum actual draught','Cargo','Latitude','Longitude','Time stamp','SOG','COG','Heading','Nav Status'], dtype=typeAisNO )
	df2=df[['MMSI','Ship name','Call sign','IMO number','Ship type','Destination','Maximum actual draught','Cargo','Latitude', 'Longitude']]
	df2['Unix time']=df2.shape[0]*pd.np.nan
	df2['SOG']=df['SOG']
	df2['COG']=df['COG']
	df2['Heading']=df['Heading']
	df2['Nav status']=df['Nav Status']
	df2['ROT']=df2.shape[0]*pd.np.nan
	df2['A']=df['Length']
	df2['B']=df2.shape[0]*pd.np.nan
	df2['C']=df['Breadth']
	df2['D']=df2.shape[0]*pd.np.nan
	df2['Time stamp']=df['Time stamp']
	df2['Expected time']=df2.shape[0]*pd.np.nan
	df2['Signal type']=df2.shape[0]*pd.np.nan
	df2['Ais']=df2.shape[0]*pd.np.nan
	return df2


def DMA(filepath):
	df=pd.read_csv(filepath, sep = "\t", header=None, names=["MMSI","Ship name","Call sign","IMO number","Ship type","Destination","Maximum actual draught","Cargo","Latitude","Longitude","Unix time","SOG","COG","Heading","Nav Status","ROT","A","B","C","D","Time stamp","Empty"], dtype=typeDMA)
	df2=df[["MMSI","Ship name","Call sign","IMO number","Ship type","Destination","Maximum actual draught","Cargo","Latitude","Longitude","Unix time","SOG","COG","Heading","Nav Status","ROT","A","B","C","D","Time stamp"]]
	df2['Expected time']=df2.shape[0]*pd.np.nan
	df2['Signal type']=df2.shape[0]*pd.np.nan
	df2['Ais']=df2.shape[0]*pd.np.nan
	return df2
