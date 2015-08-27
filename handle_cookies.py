

#! /usr/bin/env python
# Reading the cookie's from Firefox/Mozilla. Supports Firefox 3.0 and Firefox 2.x
#
# Author: Noah Fontes <nfontes AT cynigram DOT com>, 
#         Tim Ansell <mithro AT mithis DOT com>
# License: MIT
 
import cookielib
import os
import sys
import logging
import ConfigParser
 
# Set up cookie jar paths
def _get_firefox_cookie_jar (path):
    profiles_ini = os.path.join(path, 'profiles.ini')
    if not os.path.exists(path) or not os.path.exists(profiles_ini):
        return None
 
    # Open profiles.ini and read the path for the first profile
    profiles_ini_reader = ConfigParser.ConfigParser();
    profiles_ini_reader.readfp(open(profiles_ini))
    profile_name = profiles_ini_reader.get('Profile0', 'Path', True)
 
    profile_path = os.path.join(path, profile_name)
    if not os.path.exists(profile_path):
        return None
    else:
        if os.path.join(profile_path, 'cookies.sqlite'):
            return os.path.join(profile_path, 'cookies.sqlite')
        elif os.path.join(profile_path, 'cookies.txt'):
            return os.path.join(profile_path, 'cookies.txt')
 
def _get_firefox_nt_cookie_jar ():
    # See http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/473846
    try:
        import _winreg
        import win32api
    except ImportError:
        logging.error('Cannot load winreg -- running windows and win32api loaded?')
    key = _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, r'Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders')
    try:
        result = _winreg.QueryValueEx(key, 'AppData')
    except WindowsError:
        return None
    else:
        key.Close()
        if ret[1] == _winreg.REG_EXPAND_SZ:
            result = win32api.ExpandEnvironmentStrings(ret[0])
        else:
            result = ret[0]
 
    return _get_firefox_cookie_jar(os.path.join(result, r'Mozilla\Firefox\Profiles'))
 
def _get_firefox_posix_cookie_jar ():
    return _get_firefox_cookie_jar(os.path.expanduser(r'~/.mozilla/firefox'))
 
def _get_firefox_mac_cookie_jar ():
    # First of all...
    result = _get_firefox_cookie_jar(os.path.expanduser(r'~/Library/Mozilla/Firefox/Profiles'))
    if result == None:
        result = _get_firefox_cookie_jar(os.path.expanduser(r'~/Library/Application Support/Firefox/Profiles'))
    return result

def getFirefoxCookie():
    FIREFOX_COOKIE_JARS = {
        'nt': _get_firefox_nt_cookie_jar,
        'posix': _get_firefox_posix_cookie_jar,
        'mac': _get_firefox_mac_cookie_jar
    }
     
    cookie_jar = None
    try:
        cookie_jar = FIREFOX_COOKIE_JARS[os.name]()
    except KeyError:
        cookie_jar = None
     
    path = os.environ['HOME'] + '/Library/Application Support/Firefox/Profiles/nvoog4xy.default/cookies.sqlite'
    # raw_input('Path to cookie jar file [%s]: ' % cookie_jar)
    if path.strip():
        # Some input specified, set it
        cookie_jar = os.path.realpath(os.path.expanduser(path.strip()))
     
    if cookie_jar.endswith('.sqlite'):
        cookie_jar = sqlite2cookie(cookie_jar)
    else:
        cookie_jar = cookielib.MozillaCookieJar(cookie_jar)

    return cookie_jar

    """Returns a CookieJar from a key/value dictionary.

    :param cookie_dict: Dict of key/values to insert into CookieJar.
    """
    if cookiejar is None:
        cookiejar = RequestsCookieJar()

    if cookie_dict is not None:
        for name in cookie_dict:
            cookiejar.set_cookie(create_cookie(name, cookie_dict[name]))
    return cookiejar

def sqlite2cookie(filename):
    from cStringIO import StringIO
    from pysqlite2 import dbapi2 as sqlite
    # from sqlite import dbapi2 as sqlite
 
    con = sqlite.connect(filename)
 
    cur = con.cursor()
    cur.execute("select host, path, isSecure, expiry, name, value from moz_cookies")
 
    ftstr = ["FALSE", "TRUE"]
 
    s = StringIO()
    s.write("""\
# Netscape HTTP Cookie File
# http://www.netscape.com/newsref/std/cookie_spec.html
# This is a generated file!  Do not edit.
""")
    for item in cur.fetchall():
        s.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
            item[0], ftstr[item[0].startswith('.')], item[1],
            ftstr[item[2]], item[3], item[4], item[5]))
 
    s.seek(0)
 
    cookie_jar = cookielib.MozillaCookieJar()
    cookie_jar._really_load(s, '', True, True)
    return cookie_jar
 
def set_cookies_from_text(cookie_txt):
    cj = cookielib.LWPCookieJar()
    for it in cookie_txt:
        keys,values=[],[]
        result = dict(
            version=0,
            name='',
            value='',
            port=None,
            domain='',
            path='/',
            secure=False,
            expires=None,
            discard=True,
            comment=None,
            comment_url=None,
            rest={'HttpOnly': None},
            rfc2109=False,)
    
        for k,v in it.iteritems():
            if [result.keys()].count(k) != 0:
                result[k]=v
    
        result['port_specified'] = bool(result['port'])
        result['domain_specified'] = bool(result['domain'])
        result['domain_initial_dot'] = result['domain'].startswith('.')
        result['path_specified'] = bool(result['path'])
        #result=dict(zip(keys,values))
        ck = cookielib.Cookie(**result)
        cj.set_cookie(ck)