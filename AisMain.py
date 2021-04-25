import argparse
import os
import pandas as pd
from omformer import AisDK, AisNO, DMA
from sqlalchemy import create_engine
import time
import progressbar





def upload_file(connection, omformer, filepath, projectname, country, area, source, directory, dirstring):
	df = omformer(filepath)

	# Insert meta data columns
	df["Project"] = projectname
	df["Country"]	  = country 
	df["Area"]		  = area
	df["Source"]	  = source
	df["Diretory"]	  = directory
	
	df.to_sql("AisData", connection, if_exists='append', index=False, index_label="id", chunksize=1000000)	

def upload(dir, files, projectname, country, area, source, directory, dirstring):
	
	connection = engine.connect()
	trans = connection.begin()
	
	if source == "AisDK":
		omformer = AisDK
	elif source == "AisNO":
		omformer = AisNO
	elif source == "DMA":
		omformer = DMA
	else:
		raise ValueError("Error while uploading - Unknown source type '{}'".format(source))
	
	print("\nBeginning upload of directory '{}' to database\n".format(directory))
	
	bar = progressbar.ProgressBar(max_value=len(files), widgets=[
		' [', progressbar.Timer(), '] ',
		progressbar.Bar(),
		' (', progressbar.ETA(), ') ',
	])
	
	try:
		for i, file in enumerate(files):
			filepath = os.fsdecode(dir) + "\\" + file
			print("Processing {s} ({i}/{l})".format(s=filepath, i=i+1, l=len(files)))
			time.sleep(0.1)
			bar.update(i)
			upload_file(connection, omformer, filepath, projectname, country, area, source, directory, dirstring)
			
		bar.update(i+1)	
		trans.commit()
		print("\n\n'{}' uploaded succesfully! Closing".format(directory))
		connection.close()
	except:
		trans.rollback()
		connection.close()
		print("Failed during upload of '{}'... Rolling back any data transfered. Closing".format(directory))
		raise
		

	
	
def export(query, fname, delimiter):

	print("\nBeginning export of data\n")
	connection = engine.connect()
	
	column_names = ["MMSI", 'Ship name', 'Call sign', 'IMO number', 'Ship type', "Destination", 'Maximum actual draught', "Cargo", "Latitude", "Longitude", 'Unix time', "SOG", "COG", "Heading", 'Nav Status', "ROT", "A", "B", "C", "D", 'Time stamp', 'Expected time', 'Signal type', "Ais"]
	
	sql = 'SELECT {} FROM AisData WHERE '.format('"'+'", "'.join(column_names)+'"')+query
	
	print("\nExecuting query {}\n".format(sql))
	
	iterator = pd.read_sql(sql, connection, coerce_float=False, chunksize=1000000)

	try:
		df = pd.concat(iterator, ignore_index=True)
		if not fname.endswith(".csv"): fname += ".csv"
		df.to_csv(fname, sep=delimiter, header=True, index=False)
		print("\nFinished! Results written to {}. Closing.".format(fname))
		connection.close()
		
	except ValueError:
		print("\nNo matches for query '{}' found. Closing.".format(query))
		connection.close()
	
	
	
if __name__ == "__main__":

	# Parse arguments from terminal
	parser = argparse.ArgumentParser(description="Access the AisDatabase. Currently supports upload or export as mode.")
	
	parser.add_argument("mode", help="specify what action to perform (either status, upload, export or remove)")
	
	# Upload arguments
	parser.add_argument("-p", "--project", help="If mode==upload, specifies the project name of the diretory to be uploaded. Spaces not allowed.", metavar="1234567890")
	
	parser.add_argument("-s", "--span", help="If mode==upload, specifies the start/end date of project. Spaces not allowed.", metavar="DD.MM.YYYY/DD.MM.YYYY")
	
	parser.add_argument("-c", "--country", help="If mode==upload, specifies the country of project. Spaces not allowed.", metavar=["DK", "NO"])
	
	parser.add_argument("-a", "--area", help="If mode==upload, specifies the area of the project. Spaced allowed.", nargs='+', metavar=["Roskilde Fjord", "Helsingør"])
	
	parser.add_argument("-sc", "--source", help="If mode==upload, specifies the source of the project data. Spaces not allowed.", metavar=["AisDK", "DMA", "AisNO"])
	
	parser.add_argument("-d", "--directory", help="If mode==upload, specifies the directory to be uploaded. Spaces allowed.", nargs='+', metavar="C:\\user\\documents\\uploadedfolder")
	
	# Export arguments
	parser.add_argument("-q", "--query", help="If mode==export, specifies the WHERE clause of a sql query of exported data.", nargs='+')
	
	parser.add_argument("-o", "--out", help="If mode==export, specifies the name of the output .csv file. Saved into the /out folder of the project.", nargs='+')
	
	parser.add_argument("-dl", "--delimiter", help="If mode==export, specifies the delimiter of the output .csv file.")
	
	
	args = parser.parse_args()
	
	engine = create_engine("sqlite:///AisDatabase.db") #sqlite:///\\streep\bredesager\RISK.REL\AISDatabase\AisDatabase.db
	
	pd.options.mode.chained_assignment = None
	
	yes = ["", "y", "yes", "Y", "YES"]
	no  = ["n", "no", "N", "NO"]
	
	if args.mode == "status":
		pass
	
	
	elif args.mode == "upload":
		if not args.project or not args.span or not args.country or not args.area or not args.source or not args.directory:
			print("\nCould not upload data to database!\n\t- Please specify all the argument -p, -s, -c, -a, -sc and -d\n\t- Run 'python {} -h' for help to how to specify these variables".format(parser.prog))
		else:
		
			projectname = args.project
			span = args.span
			country = args.country
			area = " ".join(args.area)
			source = args.source
			
			dirstring = " ".join(args.directory)
			dir = os.fsencode(dirstring)
			files = []
			folder = dirstring[dirstring.rfind("\\")+1:]
			
			for file in os.listdir(dir):
				filename = os.fsdecode(file)
				if filename.endswith(".csv") or filename.endswith(".tsv"): files.append(filename)
			
			outfiles=""
			nfiles=""
			if len(files)>10:
				outfiles = "'"+"',\n\t\t\t\t\t'".join(files[0:3])+"',\n\t\t\t\t\t [...]"+"\n\t\t\t\t\t'"+"',\n\t\t\t\t\t'".join(files[-3:])+"'"
			else:
				outfiles = "'"+"',\n\t\t\t\t\t'".join(files)+"'"
			
			if len(files)>1:
				nfiles="s ({})".format(len(files))
			
			print("\nInitiating upload of data to AisDK:\n\t- Directory to be uploaded:\t'{d}'\n\n\t- File{n} to be uploaded:\t{f}".format(d=folder, n=nfiles, f=outfiles))
			print("\n\t- Project name:\t'{}'".format(projectname), end="\n")
			print("\n\t- Project span:\t'{}'".format(span))
			print("\n\t- Country:\t'{}'".format(country))
			print("\n\t- Area:\t\t'{}'".format(area))
			print("\n\t- Source:\t'{}'".format(source))
			
			proceed = input("\nDo you wish to proceed with the upload? ([y]/n): ")
			
			while proceed not in yes and proceed not in no:
				proceed = input("Do you wish to proceed with the upload? ([y]/n): ")
			
			if proceed in yes:
				upload(dir, files, projectname, country, area, source, folder, dirstring)
			else:
				print("Directory not uploaded to database. Closing...")
		
		
	elif args.mode == "export":
		if not args.query or not args.out or not args.delimiter:
			print("\nCould not export data from database!\n\t- Please specify all the argument -q, -o and -dl\n\t- Run 'python {} -h' for help to how to specify these variables".format(parser.prog))
		else:
			query = " ".join(args.query)
			out = " ".join(args.out)
			
			
			print("\nInitiating export of data from AisDK:\n\t- Output file\t'{d}'\n\n\t- Delimiter:\t{f}\n\n\t- Query:\t{q}".format(d=out, f=args.delimiter, q=query))
			proceed = input("\nDo you wish to proceed with the export? ([y]/n): ")
			
			while proceed not in yes and proceed not in no:
				proceed = input("Do you wish to proceed with the export? ([y]/n): ")
			
			if proceed in yes:
				export(query, out, args.delimiter)
			else:
				print("Export not initiated. Closing...")
				
	
	
	elif args.mode == "remove":
		pass