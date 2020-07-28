#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 12 16:03:32 2020

@author: ashishdwivedi
"""


import sys, getopt, csv, os, logging
from collections import namedtuple
import xlrd
import pandas as pd

SCRIPT_NAME = 'xlsTobib'
LOG_FILENAME = 'xlsTobib.log'

# base_path = '/Users/ashishdwivedi/Documents/publications/'
base_path = 'C\:\\Users\\zoterotest\\'

logger = logging.getLogger('xls2biblogger')
logger.setLevel(logging.ERROR)
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=20, backupCount=1)
logger.addHandler(handler)

BIB_FIELDS_MAP = {
  'address':      ['address', 'place of publication'],
  'author':       ['author', 'authors','author_1','co_authors'],
  'bookTitle':    ['title_source_book'],
  'date'    :     ['date_publication','year','date'],
  'editor':       ['editor', 'editors','editor(s)'],
  'institution':      ['university'],
  'key':   ['filename'],
  'npa':        ['not_protected _areas_cited','not_protected_areas_cited'],
  'number':	  ['issue number'],
  'pa':        ['protected_areas_cited'],
  'pages':        ['pagination'],
  # 'publication': ['publication', 'journal'],
  'journal':  ['journal'],
  'publisher':    ['publisher'],
  'series':       ['series', 'series and - if applicable - series number'],
  'tags':         [],
  'title':        ['title', 'book title', 'contribution title', 'article title'],
  'type':         ['type', 'report type'],
  'theme':       ['theme'],   
  'volume':       ['volume']
  
}


ALLOWED_FEILDS_BY_ITEM_TYPE = {
  
  'Inbook':['key','author','date','title','editor','booktitle','publication','publisher', 'volume',
            'pages', 'pa', 'npa', 'theme','tags','file'],
  
  'article': [ 'key','author','date','title','journal','editor','booktitle', 
              'publisher', 'volume', 'pages', 'pa', 'npa', 'theme','tags','file'],
  
  'inproceedings':[ 'key','author','date','title','publication','editor','booktitle', 
                   'publisher', 'volume','pages', 'pa', 'npa', 'theme','tags','file'],
  
  'thesis':['key','author', 'editor', 'title','bookTitle','institution university',
            'type', 'pages', 'pa', 'npa', 'theme','tags','file'],
  
  'report':[ 'key','author','date','title','editor', 'publisher', 'volume','pages', 
            'pa', 'npa', 'theme','tags','file'],
  
  'manuscript':['key','author','date','title', 'publisher', 'pages', 'pa', 'npa', 
                'theme','tags','file'],
  
  'statute':['key','author','date','title', 'publisher','volume','pages', 'pa', 'npa', 
                'theme','tags','file'],
  
  'incollection': ['key','author','date','title','publication','editor','booktitle', 
              'publisher', 'volume', 'pages', 'pa', 'npa', 'theme','tags','file'],
  
  
  
  
}


class CSVParseError(Exception):
    pass 


def parse_reference(row, attributes_order):
  ref = {}
  # print(row)
  # print(attributes_order)
  for i, col in enumerate(row):
    # Skip columns for which we did not recognise the header
    # print(i,col)  
    if i not in attributes_order:
      continue
    col = col.strip()
    if len(col) == 0:
      continue
    if ref.get(attributes_order[i]) is not None:
        if attributes_order[i] == 'author':
            col= col.replace('&', 'and \n')
            ref[attributes_order[i]] = str(ref.get(attributes_order[i])) +" and \n"+col
        else:
            ref[attributes_order[i]] = str(ref.get(attributes_order[i])) +", "+col
        if attributes_order[i] == 'file':
            # print("------\n",attributes_order[i])
            col = col+'.pdf:'+base_path+':application/pdf'
    else:
        ref[attributes_order[i]] = col
  # print(ref,"\n")      
  return ref


def get_item_type(category):
    if category.lower() == 'book section':
        return 'Inbook'
    if category.lower() == 'book':
        return 'book'
    if category.lower() == 'journal article':
        return 'article'
    if category.lower() == 'conference paper':
        return 'inproceedings'
    if category.lower() == 'thesis':
        return 'thesis'
    if category.lower() == 'report':
        return 'report'
    if category.lower() == 'unpublished report':
        return 'manuscript'
    if category.lower() == 'permis environnemental':
        return 'statute'
    
    return 'incollection'

def strip_disallowed_headers(attributes_order, refs_type, 
                             allowed_attributes_by_type=ALLOWED_FEILDS_BY_ITEM_TYPE):
  clean_attributes_order = {}
  for idx, header in attributes_order.items():
    if header in allowed_attributes_by_type[refs_type]:
      clean_attributes_order[idx] = header
  return clean_attributes_order

def parse_headers(headers, bib_attribute_map = BIB_FIELDS_MAP):
  valid_columns = {}
  invalid_columns = {}
  # print(headers)
  for i, header in enumerate(headers):
    found = False
    for bib_attribute, bib_attribute_variants in bib_attribute_map.items():
      # The reference attribute is recognised, and allowed for that reference type
      if header.lower() in bib_attribute_variants:
        valid_columns[i] = bib_attribute
        found = True
        break
    if found == False:
      invalid_column_value = header.strip()  
      if invalid_column_value is not '':
          invalid_columns[i] = invalid_column_value
  # print('key' in valid_columns.values())
  # print(valid_columns.values())

  if 'key' not in valid_columns.values():
    raise CSVParseError('no "key" column found')
  
  columns = namedtuple("columns", ["valid", "invalid"])
  # print("-----\n",columns)
  return columns(valid_columns, invalid_columns)  


def to_bib(ref, ref_type):
  bib_ref = "@%s{%s, \n" % (ref_type, ref['key'])
  for attr, attr_value in ref.items():
    if attr == 'key':
      continue
    # print("\n checking attr_value")
    # print(ref)
    # print(attr_value)
    for one_author in attr_value.split(';'):
      bib_ref += '  %s = "%s",\n' %(attr, one_author.strip())
      # print(one_author)
  # print(ref['key'])    
  bib_ref +=' %s = {%s} ,\n'%('file',ref['key']+'.pdf:'+base_path+ref['key']+'.pdf:application/pdf')    
  bib_ref += ' %s = {%s} \n'%('keyword',build_tags(ref))
  bib_ref += "}\n"
  build_tags(ref)
  return bib_ref

def build_tags(ref):
    keyword = ''
    if ('pa' in ref):
        # print(ref.get('pa'), ref['pa'])
        key_values = ref.get('pa').split(',')
        key_values = ['pa:' + v.strip() for v in key_values]
        keyword += ','.join(key_values)+','
        # print(keyword)
    if('theme' in ref):
        key_values = ref.get('theme').split(',')
        key_values = ['theme:' + v.strip() for v in key_values]
        keyword += ','.join(key_values)+','
        # print(keyword)
    if('npa' in ref):
        key_values = ref.get('npa').split(',')
        key_values = ['npa:' + v.strip() for v in key_values]
        keyword += ','.join(key_values)+','
        # print(keyword)
    keyword +=','
    keyword = keyword. replace(',,','')
    # print(keyword)
    return keyword

def csv_to_bib(csv_file, delimiter, item_type):
  recognised_columns = {}
  bib_refs = []
  bib_file_name = 'bibFile.bib' 

  with open(csv_file, 'r') as f:
    refs_type = ''
    csv_reader = csv.reader(f, delimiter=delimiter, quotechar='"')

    for row in csv_reader:
      if len(''.join(row)) == 0: # skip leading empty lines
        continue
      
      if len(recognised_columns) == 0:
        # We assume all refs in a CSV are the same type (book, article, ...)  
        columns = parse_headers([x.lower() for x in row])
        if len(columns.invalid) > 0:
            for idx in columns.invalid:
                print('Warning in file %s: unrecognised column %s' % (csv_file, columns.invalid[idx]), file=sys.stderr)
        recognised_columns = strip_disallowed_headers(columns.valid, item_type)
        continue
      # print(columns.valid)
      # print(columns.invalid,"<--\n")
      reference = parse_reference(row, columns.valid)
      bib_refs.append(to_bib(reference, item_type))
    with open(bib_file_name, 'a+') as bibtex_file:
        bibtex_file.write("\n".join(bib_refs))
        bibtex_file.close()
  return "\n".join(bib_refs)




def main(argv, item_type):
  failure = 0
  delimiter = ','
  csv_file = argv
  try:
      csv_to_bib(csv_file, delimiter, item_type)
    # print (csv_to_bib(csv_file, delimiter))
  except CSVParseError as e:
      print ('Error: Failed to parse %s: %s' % (csv_file, str(e)), file=sys.stderr)
      failure = 1
  except FileNotFoundError as e:
      print ('Error: Failed to parse %s: file not found' % csv_file, file=sys.stderr)
      failure = 1

def fetchCSVFile(workbook_name):
    workbook = xlrd.open_workbook(workbook_name)
    sheets_in_workbook = workbook.sheet_names()
    for sheet in sheets_in_workbook:
        print(sheet)
        csv_filename = sheet+".csv"
        if sheet.lower() != 'reference':
            df=pd.read_excel(workbook_name, sheet_name = sheet)
            item_type = get_item_type(df.Category.unique()[0].lower())
            df = df.drop('Category',1)
            # print(item_type)
            df.to_csv(csv_filename)
            main(csv_filename, item_type)
            os.remove(csv_filename)  
    return

if __name__ == "__main__": 
    workbook_name = sys.argv[1]
    fetchCSVFile(workbook_name)
    # sys.exit(main(sys.argv[1]))
   
   


