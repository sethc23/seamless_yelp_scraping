from os import getcwd as os_getcwd
from sys import argv, path
from bs4 import BeautifulSoup
from urllib import quote_plus#,unquote
from os import environ as os_environ
from os import path as os_path
from sys import path as py_path
from xgoogle.search import GoogleSearch

def getSoup(x):
    if type(x) == str: return BeautifulSoup(x)
    if type(x) == unicode: return BeautifulSoup(x)
    else: return BeautifulSoup(x.prettify())

def FindAllTags(text,tag):
    a=getSoup(text)
    return a.findAll(tag)

def getAllTag(text, tag):
    h = BeautifulSoup(text)
    get_tags=['href','img']
    if get_tags.count(tag) != 0:
        x = h('a')
        a = []
        for it in x:
            a.append(it.get(tag))
    else:
        x = h(tag)
        a = []
        for it in x:
            s, l = '', it.contents
            for pt in l: s += str(pt)
            a.append(s)
    return a

def getInnerElement(text, tag, tag_var):
    soup = BeautifulSoup(text)
#     get_tags=['a','img']
#     if get_tags.count(tag) != 0:
    s='result = soup.'+tag+"['"+tag_var+"']"
    try:
        exec s
    except:
        print s
        print text
        exec s
    return result

def getTagContents(text, tag, tag_var=""):
    soup = BeautifulSoup(text)
    if tag == 'class': return str(soup(tag, id='"' + tag_var + '"')[0].contents)   
    if tag_var == 'text': return soup(tag)[0].contents

def getTagsByAttr(text, tag, attr='',contents=True):
    """
    #  attr = { "class" : "sister" }
    """
    soup = BeautifulSoup(text)
    if attr=='': a=soup.find_all(tag)
    else: a=soup.find_all(tag, attr)
    if contents==True:  return str(a[0].contents)
    if contents == False: return a

def getInnerHTML(html):
    return "".join([str(x) for x in html.contents]) 

def readTable(table):
    soup = BeautifulSoup(table)
    rows=soup.findAll('tr')
    table=[]
    for j in rows:
        jrow=[]
        for i in j.findAll('td'):
            jrow.append(i.contents)
        table.append(jrow)
    print type(table),len(table[1]),table[1]
    return table

def safe_url(url):
    return quote_plus(url)

def remove_non_ascii(text):
    return re_sub(r'[^\x00-\x7F]+',' ', text)

class google:

    def __init__(self):
        self.gs = GoogleSearch('')
    
    def get_results(self,src):
        if src != '': 
            return self.gs._extract_results(BeautifulSoup(src))
        


class Write_HTML:
    """methods for writing html code"""

    def addTag(tag, text, option=''):
        if option != '': return '<' + tag + ' ' + option + '>' + text + '</' + tag + '>'
        else: return '<' + tag + '>' + text + '</' + tag + '>'

    def horizontalRule():
        return '<hr />'

    def html_space():
        return '&nbsp;'

    def hyperlink(title, link, place='_blank'):
        text = '<a href="' + link + '"'
        text = text + ' target="' + place + '">'
        text = text + title + '</a>'
        return text

    def tableRow(columnData, widths=False):
        row = ''
        for i in range(0, len(columnData)):
            it = columnData[i]
            text = addTag('td', it)
            if type(widths) != bool:
                if ((len(columnData) != len(widths))
                    or (type(columnData) != type(widths))):
                    print 'writeHTML.tableRow error'
                    print len(columnData), ' != ', len(widths)
                    print type(columnData), ' != ', type(widths)
                    return 'error'
                else:
                    text = text.replace('<td>', '<td width="' + widths[i] + '">')
            row = row + text + '\n'
        return row

    def makeTableFromList(list_data, list_options, width="450", border="1", align="center"):
        # if type(list_data[0]) != list: list_data = [list_data]
        if list_options == '': list_options = [[''] for it in list_data]
        text = '<table width="' + width + '" border="' + border + '" align="' + align + '">\n'
        for i in range(0, len(list_data)):
            row = list_data[i]
            row_option = list_options[i]
            col_data = ''
            if type(row) == list:
                for j in range(0, len(row)):
                    col = row[j]
                    col_option = row_option[j]
                    if col_option == '': pt = addTag('td', col) + '\n'
                    else: pt = addTag('td', col, col_option) + '\n'
                    col_data = col_data + pt
            else:
                col_data = addTag('td', row) + '\n'
            row_data = addTag('tr', col_data) + '\n'
            text = text + row_data
        text = text + '</table>\n'
        return text

    def makeTableFromRows(list_data, width="200", border="1"):
        text = '<table width="' + width + '" border="' + border + '" cellpadding="0" cellspacing="0">'
        data = ''
        for row in list_data:
            data = addTag('tr', row)
            text += data + '\n'
        text = text + '</table>'
        return text

    def makeTableFromTableRows(tableRows, width="100%", border="1"):
        text = '<table width="' + width + '" border="' + border + '" cellpadding="0" cellspacing="0">'
        text += tableRows
        return text + '</table>'

    def makeListFromList(list_data):
        text = '<ul>'
        for it in list_data:
            data = '<li>' + it + '</li>\n'
            text = text + data
        text = text + '</ul>\n'
        return text

    def makeHTML(text, title='Untitled Document'):
        header = ('''
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">\n
        <html xmlns="http://www.w3.org/1999/xhtml">\n
        <head>\n
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />\n
        <title>#TITLE#</title>\n
        </head>\n
        ''')
        header = header.replace('#TITLE#', title)
        body = addTag('body', text)
        html = header + body + '\n' + '</html>'
        return html


