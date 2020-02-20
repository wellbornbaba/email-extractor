import os
from pymy import *
from bs4 import BeautifulSoup as bs
import threading
from threading import Lock
import pandas as pd
# Define the lock globally to avoid the "recursive use of cursors not allowed" error
lock = Lock()
keywords = 'student email address'  #enter keywords
next_no_pages = 10 #number of next pages to seach for note increment in 10th e.g: 50

class EmailExtractor():

	def __init__(self, keywords='', next_no_pages=10):
		self.keywords = keywords.replace(' ', '+')
		self.parsingFinished = ''
		lock.acquire(True)
		self.dbname = 'EmailDB.db'
		count = 0
		pages = []

		if not self.keywords:
			print('Please enter keywords to search for')

		else:
			db = Db(resource_path(self.dbname))
			create_email_table = """ CREATE TABLE IF NOT EXISTS emailaddress (
										id integer PRIMARY KEY,
										email text NOT NULL,
										url text NOT NULL,
										status integer
									); """

			db.createdb(create_email_table)

			while count < next_no_pages:
				url = f'https://www.google.com/search?q={self.keywords}&oq={self.keywords}&start={count}'
				pages.append(url)
				count += 10

			#start parsing google
			for searchUrl in pages:
				dcontent = remoteread_file(searchUrl)
				# do processing of others which are not in cssjs list
				content = bs(dcontent, 'html.parser')

				for foundhtml in content.find_all('div', id='search'):
					for atag in foundhtml.find_all('a'):
						try:
							foundurl = atag["href"].replace('/search?q=related:', '')
							if not foundurl == '#':
								self.startExtraction(foundurl)

						except Exception as e:
							print(f' |->Error parsingA HREF TAG: ' + str(e))

			#save datas
			self.saveData()

	def startExtraction(self, url):
		if url:
			try:
				db = Db(resource_path(self.dbname))
				notallow = ['.pdf', '.xls', '.csv', '.png', '.git', '.gif', '.jpg']
				extens = exts(url)
				if not  extens in notallow:
					text = remoteread_file(url)
					# do processing of others which are not in cssjs list
					textcontent = bs(text, 'html.parser')

					for eachemail in findemail(textcontent):
						checkdb = db.check('id', 'emailaddress', F"email='{eachemail}' ")
						if checkdb is None:
							print(' |->found/saved: ' + eachemail)
							db.insert('emailaddress', 'id, email, url, status', f"NULL,'{eachemail}', '{url}', '0'")

					#we have to research  any tag that contain words like contact or about
					for getabout_contactus_pages in textcontent.find_all('a'):
						foundUrl_href = getabout_contactus_pages['href']
						if 'about' in foundUrl_href or 'contact' in foundUrl_href:
							#proceed parsing either aboutus page or contact us page url
							if not is_url(foundUrl_href):
								#clean url
								#firstly try to get the domain
								maindomain = urldomains(url)
								foundhref = cleanurl(maindomain, foundUrl_href)
							else:
								foundhref = foundUrl_href

							try:
								sub_text = remoteread_file(foundhref)
								# do processing of others which are not in cssjs list
								sub_textcontent = bs(sub_text, 'html.parser')

								for sub_eachemail in findemail(sub_textcontent):
									sub_checkdb = db.check('id', 'emailaddress', F"email='{sub_eachemail}' ")
									if sub_checkdb is None:
										print(' |->found/saved: ' + sub_eachemail)
										db.insert('emailaddress', 'id, email, url, status', f"NULL,'{sub_eachemail}', '{foundhref}', '0'")

							except Exception as e:
								print('Sub Extracting failed: ' + str(e))

			except Exception as e:
				print('Error Occurred: '+ str(e))

			try:
				lock.release()
			except Exception:
				pass

	def saveData(self):
		xls = pd.DataFrame(columns=['Email', 'Link'])
		data = {}
		txtemail = ''
		try:
			db = Db(resource_path(self.dbname))
			getEmails = db.fetch("SELECT * FROM emailaddress ORDER BY id ASC")
			for getemail in getEmails:
				data['Email'] = getemail['email']
				data['Link'] = getemail['url']
				txtemail += getemail['email'] + '\n'
				xls = xls.append(data, ignore_index=True)
				xls.index += 1

			searchname = self.keywords.replace('+', '-')
			saveto_path = joinpath(os.getcwd(), searchname)
			xls.to_excel(saveto_path + '.xlsx')  #save as excel
			localwrite_file(saveto_path + '.txt', txtemail)  #save as txt
			#delete Email table datax
			db.others("DELETE FROM emailaddress  ")

		except Exception as e:
			print('Saving error: ' + str(e))

		try:
			lock.release()
		except Exception:
			pass


if __name__ == "__main__":
	##start extracting
	EmailExtractor(keywords, next_no_pages)
