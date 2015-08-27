
from os                                     import environ                  as os_environ
from handle_cookies                         import getFirefoxCookie,set_cookies_from_text
from time                                   import time                     as TIME
from time                                   import sleep                    as delay
from ipdb                                   import set_trace                as i_trace
from random                                 import randrange

class Mechanize:
    
    def __init__(self,browser,cookies):
        from mechanize import Browser
        br = Browser()
        # Cookie Jar
        if cookies == "firefox": cj = getFirefoxCookie()
        else: cj=set_cookies_from_text(cookies)
        br.set_cookiejar(cj)
        # Browser options 
        br.set_handle_equiv(True) 
        #br.set_handle_gzip(True) 
        br.set_handle_redirect(True) 
        br.set_handle_referer(True) 
        br.set_handle_robots(False)
        # Debug Options
        #br.set_debug_http(True) 
        #br.set_debug_redirects(True) 
        #br.set_debug_responses(True)
        # Follows refresh 0 but not hangs on refresh > 0 
        br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
        # User-Agent
        br.addheaders = [('user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.45 Safari/537.36'),
        ('accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')]
        self.browser = br
        self.browserType = 'mechanize'

    def get_page(self, gotoUrl):
        br=self.browser
        br.open(gotoUrl)
        return br.response().read()

    def get_file(self,filePath,savePath):
        self.browser.retrieve(filePath, savePath,timeout=3600)[0]

class Webdriver:

    def __init__(self,_parent,browser=None):
        self._parent                        =   _parent
        self.T                              =   _parent.T
        self.type                           =   browser
        assert ['chrome','firefox','phantom'].count(browser)>0
        self.window                         =   getattr(self,'set_%s' % browser)()
        self.window_cfg                     =   self.T
        import pickle
        from selenium.webdriver.common.keys import Keys
        self.pickle                         =   pickle
        self.keys                           =   Keys

    def config_browser(self,browser,kwargs):
        browser_config                      =   {} if not kwargs.has_key('browser_config') else kwargs['browser_config']
        if browser_config.has_key('window_position'):
            browser.set_window_position(        browser_config.window_position)

        if browser_config.has_key('window_size'):
            browser.set_window_size(            browser_config.window_size)
        else:
            browser.set_window_size(            800,1000)

        if browser_config.has_key('maximize_window') and browser_config['maximize_window']:
            browser.maximize_window(            )

        if browser_config.has_key('implicitly_wait'):
            browser.implicitly_wait(            browser_config.implicitly_wait)
        else:
            browser.implicitly_wait(            120)

        if browser_config.has_key('page_load_timeout'):
            browser.set_page_load_timeout(      browser_config.page_load_timeout)
        else:
            browser.set_page_load_timeout(      150)

        return

    def set_firefox(self,with_profile=False,**kwargs):
        from selenium           import webdriver
        if with_profile==True:
            p                   =   webdriver.firefox.firefox_profile.FirefoxProfile()
            p.set_preference(       'browser.helperApps.neverAsk.saveToDisk', ('application/pdf'))
            p.set_preference(       'browser.helperApps.neverAsk.saveToDisk','application/pdf,application/x-pdf')
            p.set_preference(       'browser.download.folderList', 2)
            p.set_preference(       'browser.download.dir', '/Users/admin/Desktop')
            p.set_preference(       'browser.download.manager.showWhenStarting', False)
            p.set_preference(       'extensions.blocklist.enabled', False)
            p.set_preference(       'pdfjs.disabled', True)
            self.browser        =   webdriver.Firefox(firefox_profile=profile)
        else:
            # profile = webdriver.firefox.firefox_profile.FirefoxProfile()
            self.browser        =   webdriver.Firefox()

    def set_chrome(self,**kwargs):
        """
        ----------------------------------------------------------------------------

        Configuration Method:
            1. EXECUTABLE,SERVICE ARGS and PORT
            2. DESIRED CAPABILITIES
            3. CHROME OPTIONS


        Command Line Switches:
            http://peter.sh/experiments/chromium-command-line-switches/

        Capabilities:
            https://sites.google.com/a/chromium.org/chromedriver/capabilities


        ----------------------------------------------------------------------------
        This function should be a class for webdriver.
        For now, just setting up Chrome.

        driver_browsers                     =   ['android',
                                                 'chrome',
                                                 'firefox',
                                                 'htmlunit',
                                                 'internet explorer',
                                                 'iPhone',
                                                 'iPad',
                                                 'opera',
                                                 'safari']

        """

        def set_defaults(self):
            default_settings                =   {'bin_path'                             :   '/usr/local/bin/chromedriver',
                                                 'port'                                 :   15010,
                                                 'log_path'                             :   os_environ['BD'] + '/html/logs/chromedriver.log',
                                                 'user-agent'                           :   "Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1",
                                                 # 1 in 1788 per panopticlick !!
                                                 'no_java'                              :   True,
                                                 'no_plugins'                           :   True,
                                                 'net-log-capture-mode'                 :   'IncludeCookiesAndCredentials',
                                                 'log-level'                            :   0,
                                                 'cookie_content'                       :   {},
                                                 'capabilities'                         :
                                                     {  'acceptSslCerts'                :   True,
                                                        'databaseEnabled'               :   False,
                                                        'unexpectedAlertBehaviour'      :   "accept",
                                                        'applicationCacheEnabled'       :   False,
                                                        'webStorageEnabled'             :   False,
                                                        'browserConnectionEnabled'      :   False,
                                                        'locationContextEnabled'        :   True,
                                                        },
                                                 'loggingPrefs'                         :
                                                     {  "driver"                        :   "ALL",
                                                        "server"                        :   "ALL",
                                                        "browser"                       :   "ALL"},
                                                 'true_opts'                            :
                                                     [
                                                         'disable-core-animation-plugins',
                                                         'disable-plugins',
                                                         'disable-extensions',
                                                         'disable-plugins-discovery',
                                                         'disable-site-engagement-service',
                                                         'disable-text-input-focus-manager',

                                                         'enable-account-consistency',
                                                         'enable-devtools-experiments',
                                                         'enable-logging',
                                                         'enable-network-information',
                                                         'enable-net-benchmarking',
                                                         'enable-network-portal-notification',

                                                         'enable-strict-site-isolation',
                                                         'incognito',                           # if incognito, extensions must be disabled
                                                         'log-net-log',
                                                         'scripts-require-action',
                                                         'system-developer-mode',
                                                         # 'use-mobile-user-agent',
                                                     ],
                                                 'false_opts'                           :
                                                     [
                                                         'enable-profiling',
                                                         ],
                                                 }
            excluded                        =   [] if not (hasattr(self,'T') and hasattr(self.T,'excluded_defaults')) else self.T.excluded_defaults
            for k,v in default_settings.iteritems():
                if excluded.count(k):
                    if T.has_key(k):
                        del T[k]
                else:
                    T.update(                   {k                                      :   v})
            return T
        def set_desired_capabilities(self):
            from selenium.webdriver             import DesiredCapabilities
            dc                              =   DesiredCapabilities.CHROME.copy()
            platforms                       =   ['WINDOWS', 'XP', 'VISTA', 'MAC', 'LINUX', 'UNIX', 'ANDROID', 'ANY']

            # -PROXY OBJECT
            # from selenium.webdriver import Proxy

            # -READ-WRITE CAPABILITIES
            rw_capabilities                 =   [
                                                 'acceptSslCerts',              # boolean unless specified
                                                 'javascriptEnabled',
                                                 'databaseEnabled',
                                                 'proxy',                       # Proxy Object
                                                 'unexpectedAlertBehaviour',    # string {"accept", "dismiss", "ignore"}
                                                 'applicationCacheEnabled',
                                                 'webStorageEnabled',
                                                 'rotatable',
                                                 'browserConnectionEnabled',
                                                 'locationContextEnabled',
                                                 'elementScrollBehavior',       # int (align with the top (0) or bottom (1) of the viewport)
                                                 'nativeEvents'
                                                 ]

            assert T.has_key('capabilities')
            for it in rw_capabilities:
                if T['capabilities'].has_key(it):
                    dc[it]                  =   str(T['capabilities'][it])


            # -loggingPrefs                         OBJECT (dict)
            #   "OFF",  "SEVERE", "WARNING",
            #   "INFO", "CONFIG", "FINE",
            #   "FINER","FINEST", "ALL"

            if T.has_key('loggingPrefs'):
                dc[it]                      =   T['loggingPrefs']

            return dc
        def set_profile(self):
            profile                         =   {#"download.default_directory"       :   "C:\\SeleniumTests\\PDF",
                                                 "download.prompt_for_download"     :   False,
                                                 "download.directory_upgrade"       :   True,
                                                 "plugins.plugins_disabled"         :   ["Chromoting Viewer",
                                                                                         "Chromium PDF Viewer"],
                                                                                         }
            opts.add_experimental_option(       "prefs", profile)
        def set_performance_logging(self):
            perfLogging                     =   {
                                                 "enableNetwork"                    :   True,
                                                 "enablePage"                       :   True,
                                                 "enableTimeline"                   :   True,
                                                 #"tracingCategories":<string>,
                                                 "bufferUsageReportingInterval"     :   1000
                                                }

            opts.add_experimental_option(     "perfLoggingPrefs",perfLogging)
        def set_chrome_options(self):
            from selenium.webdriver             import ChromeOptions
            opts                            =   ChromeOptions()

            ### Add Boolean Arguments
            if T.has_key('true_opts'):
                for it in T['true_opts']:
                    opts.add_argument(          '%s=1' % it )
            if T.has_key('false_opts'):
                for it in T['false_opts']:
                    opts.add_argument(          '%s=0' % it )

            value_opts                      =   [
                                                 'profile-directory',
                                                 'log-level',                   # 0 to 3: INFO = 0, WARNING = 1, LOG_ERROR = 2, LOG_FATAL = 3
                                                 'net-log-capture-mode',        # "Default" "IncludeCookiesAndCredentials" "IncludeSocketBytes"'
                                                 'register-font-files',         # might be windows only
                                                 'remote-debugging-port',
                                                 'user-agent',
                                                 'user-data-dir',               # don't use b/c it negates no-extension options
                                                 ]

            ### Add Value Arguments
            for it in value_opts:
                if T.has_key(it):
                    opts.add_argument(           '%s=%s' % (it,T[it]) )

            ### OTHER CHROME OPTIONS NOT YET FULLY CONFIGURED

            # -extensions        list str
            # -localState        dict
            # -prefs             dict
            # set_profile()

            # -detach            bool
            # -debuggerAddress   str
            # -excludeSwitches   list str
            # -minidumpPath      str
            # -mobileEmulation   dict

            # -perfLoggingPrefs             OBJECT (dict)
            # set_performance_logging()

            return opts

        from selenium.webdriver             import Chrome

        T                                   =  {}
        if kwargs:
            T.update(                           kwargs)
        if (hasattr(self,'T') and hasattr(self.T,'kwargs')):
            T.update(                           self.T.kwargs)

        # Cycle Through kwargs and Extract Configs
        if hasattr(self.T,'id'):
            T.update(                           self.T.id.__dict__)

            if hasattr(self.T.id,'details'):
                for k,v in self.T.id.details.__dict__.iteritems():
                    T.update(                   { k.strip('_')                      :   v})

            if hasattr(self.T.id,'cookie'):
                if hasattr(self.T.id.cookie,'content'):
                    T.update(                   {'cookie_content'                   :   self.T.id.cookie.content})

        # Set Defaults if not provided
        if not T.has_key('defaults'):
            T                               =   set_defaults(self)

        # Config Data Storage if Possible
        if T.has_key('SAVE_DIR'):
            T['user-data-dir']              =   T['SAVE_DIR']
            T['profile-directory']          =   'Profile'
        if T.has_key('guid'):
            T['log_path']                   =   '%s/%s.log' % (T['SAVE_DIR'],T['guid'])

        # Configure with Special Profiles if Requested
        special_profiles                    =   os_environ['BD'] + '/html/webdrivers/chrome/profiles'
        if T.has_key('no_java') and T['no_java']:
            if T.has_key('no_plugins') and T['no_plugins']:
                T['user-data-dir']          =   special_profiles + '/no_java_no_plugins/'
                del T['profile-directory']
            else:
                T['user-data-dir']          =   special_profiles + '/no_java/'
                del T['profile-directory']
        elif T.has_key('no_plugins') and T['no_plugins']:
            T['user-data-dir']              =   special_profiles + '/no_plugins/'
            del T['profile-directory']



        # SERVICE ARGS          # ( somewhat documented in executable help, i.e., chromedriver --help )
        service_args                        =   ["--verbose",
                                                 "--log-path=%(log_path)s" % T]

        dc                                  =   set_desired_capabilities(self)
        opts                                =   set_chrome_options(self)

        d                                   =   Chrome(  executable_path        =   T['bin_path'],
                                                         port                   =   T['port'],
                                                         service_args           =   service_args,
                                                         desired_capabilities   =   dc,
                                                         chrome_options         =   opts)
        d.set_window_size(                      1280,720)
        if T['cookie_content']:
            d.add_cookie(                       T['cookie_content'])

        self.config_browser(                    d,kwargs)


        return d,T

    def set_phantom(self,**kwargs):
        from selenium                       import webdriver
        from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

        if not hasattr(self,'T'):               return webdriver.PhantomJS(executable_path='/usr/local/bin/phantomjs')
        try:
            self.T                          =   kwargs['To_Class'](kwargs)
        except:
            return                              webdriver.PhantomJS(executable_path='/usr/local/bin/phantomjs')

        # CAPABILITIES
        dcap                                =   dict(DesiredCapabilities.PHANTOMJS)
        
        if not hasattr(self.T.br.user_agent,'user_agent'):
            self.T.br.user_agent            =   ("Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1")
        dcap["phantomjs.page.settings.userAgent"] = self.T.br.user_agent
        

        # SERVICE ARGS
        service_args                        =   []
        if hasattr(self.T.br.service_args,'cookie_file'):
            service_args.extend(                ['--cookies-file=%s'        % self.T.br.service_args.cookie_file])
        if hasattr(self.T.br.service_args,'debug'):
            service_args.extend(                ['--debug=%s'               % str(self.T.br.service_args.debug).lower()])
        if hasattr(self.T.br.service_args,'ignore_ssl_errors'):
            service_args.extend(                ['--ignore-ssl-errors=%s'   % str(self.T.br.service_args.ignore_ssl_errors).lower()])
        if hasattr(self.T.br.service_args,'load_images'):
            service_args.extend(                ['--load-images=%s'         % str(self.T.br.service_args.load_images).lower()])
        if hasattr(self.T.br.service_args,'local_remote_access'):
            service_args.extend(                ['--local-to-remote-url-access=%s' % str(self.T.br.service_args.local_remote_access).lower()])
        if hasattr(self.T.br.service_args,'debugger_port'):
            service_args.extend(                ['--remote-debugger-port=%s'% self.T.br.service_args.debugger_port])
        if hasattr(self.T.br.service_args,'proxy'):
            service_args.extend(                ['--proxy=%s'               % self.T.br.service_args.proxy[:7]
                                                                                if self.T.br.service_args.proxy.find('http://')==0
                                                                                else self.T.br.service_args.proxy,
                                                 '--proxy-type=http'])
        if hasattr(self.T.br.service_args,'ssl_cert_path'):
            service_args.extend(                ['--ssl-certificates-path=%s'% self.T.br.service_args.ssl_cert_path])
        if hasattr(self.T.br.service_args,'wd_log_level'):
            service_args.extend(                ['--webdriver-loglevel=%s'  % self.T.br.service_args.wd_log_level])

        # INITIATE WEB DRIVER
        d                                   =   webdriver.PhantomJS(executable_path='/usr/local/bin/phantomjs',
                                                                    desired_capabilities=dcap,
                                                                    service_args=service_args)
        # CAPABILITY CONFIG
        known_capabilities                  =   ['applicationCacheEnabled',
                                                 'locationContextEnabled',
                                                 'databaseEnabled',
                                                 'webStorageEnabled',
                                                 'javascriptEnabled',
                                                 'acceptSslCerts',
                                                 'browserConnectionEnabled',
                                                 'rotatable']
        selected_capabilities               =   [] if not hasattr(self.T.br,'capabilities') else self.T.br.capabilities
        for it in known_capabilities:
            d.capabilities[it]              =   True if selected_capabilities.count(it) else False

        self.config_browser()

        return d,self.T.br.service_args

    def Select(self,element):
        """
        Related Example:

        time_element            =   br.browser.find_element_by_id('deliveryTime')
        time_str                =   "1:00 PM"
        _select                 =   Select(time_element)
        _select.select_by_value(    time_str)
        assert time_element.get_attribute("value")==time_str

        ALSO:

        addresses               =   br.window.find_element_by_xpath("//select[@id='Address']")
        last_option             =   len(addresses.find_elements_by_tag_name('option'))-1
        addresses.find_elements_by_tag_name('option')[last_option].click()

        """
        from selenium.webdriver.support.select import Select
        return Select(element)

    def Actions(self,browser):
        """See here: https://selenium-python.readthedocs.org/api.html#module-selenium.webdriver.common.action_chains """
        from selenium.webdriver.common.action_chains import ActionChains
        # Example: action_chains.context_click(p).perform()
        return ActionChains(browser)

    def click_element_id_and_type(self,element_id,type_text):
        self.scroll_to_element(                 element_id)
        pt                                  =   self.window.find_element_by_id(element_id)
        pt_offset                           =   (pt.location['x'] + randrange(0,pt.size['width']),
                                                 pt.location['y'] + randrange(0,pt.size['height']))
        Actions                             =   self.Actions(self.window)
        Actions.move_to_element_with_offset(    pt,*pt_offset)
        Actions.send_keys_to_element(           pt,type_text)
        Actions.perform(                        )

    def execute(self,script,*args):
        return self.window.execute_script(script,*args)

    def frame_count(self):
        return self.window.frame_attr()

    def get_cookies(self):
        self.cookies=self.window.get_cookies()

    def get_url(self):
        return self.window.current_url

    def get_uniqueness_info(self):
        url                                 =   'https://panopticlick.eff.org/index.php?action=log'
        self.window.get(                        url)

    def get_element_id_val(self,element_id):
        return self.execute('return document.getElementById("%s").value;' % element_id)

    def open_page(self, gotoUrl):
        self.window.get(gotoUrl)
        #sleep(10)

    def post_screenshot(self):
        fpath                               =   os_environ['HOME'] + '/.scripts/tmp/phantom_shot'
        if self.T.THIS_PC=='ub2':   # i.e., PC running web server is also running scraper
            self.screenshot(                      fpath )
        else:
            self.screenshot(                    '/tmp/phantom_shot' )
            cmds                            =   ['scp /tmp/phantom_shot %(host)s@%(serv)s:%(fpath)s;'
                                                 % ({ 'host'                :   self.T.user,
                                                      'serv'                :   self.T.user,
                                                      'fpath'               :   fpath }),
                                                 'rm -f /tmp/phantom_shot;']
            p                               =   self.T.sub_popen(cmds,stdout=self.T.sub_PIPE,shell=True)
            (_out,_err)                     =   p.communicate()
            assert _out                    ==   ''
            assert _err                    ==   None
        return

    def quit(self):
        self.update_cookies(                    )
        self.window.quit(                       )

    def randomize_keystrokes(self,keys,element,**kwargs):
        T                                   =   {'shortest_delay_ms'        :   500,
                                                 'longest_delay_ms'         :   1200,
                                                }
        for k,v in kwargs:
            T.update(                           { k                         :   v })
        pauses                              =   map(lambda s: randrange(
                                                    T['shortest_delay_ms'],
                                                    T['longest_delay_ms'])//float(100),keys)

        for i in range(len(keys)):
            action_chain                    =   self.Actions(self.window)
            action_chain.send_keys_to_element(  element,keys[i]).perform()
            delay(                              pauses[i])
        return True

    def reset_frames(self):
        return self.window.switch_to_window(self.window.current_window_handle)

    def screenshot(self,save_path='$SHARE2/Desktop/screen.png'):
        self.window.save_screenshot(save_path)

    def scroll_to_element(self,element):
        return self.window.execute_script('document.getElementById("%s").scrollIntoView(true)'%element)

    def scroll_and_click_id(self,element_id):
        self.scroll_to_element(                 element_id)
        return self.window.find_element_by_id(element_id).click()

    def set_element_val(self,element,val,val_type):
        _str_sub                            =   '"%s"' % val if val_type is str else '%s' % val
        _script                             =   'var elem = document.getElementById("%s"); elem.value = %s;' % (element,_str_sub)
        return self.window.execute_script( _script)

    def source(self):
        return self.window.page_source

    def update_cookies(self):
        try:
            assert hasattr(self,'window_cfg') and self.window_cfg.has_key('cookie')
            _cookie                             =   self.window_cfg['cookie']
            assert _cookie.has_key('content') and _cookie.has_key('f_path')
            _current                            =   self.window.get_cookies()
        except:
            return
        try:
            _stored                         =   self.pickle.load(open(self.window_cfg['cookie']['f_path'], "rb"))
        except:
            _stored                         =   []

        if not _stored and not _current:
            pass

        elif _stored and not _current:
            for it in _stored:
                self.window.add_cookie(         it)

        elif (_current and not _stored) or ( (_current and _stored) and _current != _stored ):
            self.pickle.dump(                   _current ,
                                                open(self.window_cfg['cookie']['f_path'],"wb"))

        return

    def wait_for_condition(self,condition,param1,param2,invert=False,timeout_seconds=45,poll_frequency=0.5):
        """

            NOTE -- UNTESTED

            Usage:

                br.wait_for_condition('presence_of_element_located','id','some_tag_id_value',
                                    invert=False,timeout_seconds=45,poll_frequency=1.0)


            WebDriverWait(self, driver, timeout, poll_frequency=0.5, ignored_exceptions=None)

        """
        def element_types(element_type):
            global By
            from selenium.webdriver.common.by import By
            element_types           =  {'class_name'            :By.CLASS_NAME,
                                        'id'                    :By.ID,
                                        'link_text'             :By.LINK_TEXT,
                                        'name'                  :By.NAME,
                                        'partial_link_text'     :By.PARTIAL_LINK_TEXT,
                                        'tag_name'              :By.TAG_NAME,
                                        'xpath'                 :By.XPATH}
            assert element_types.keys().count(element_type)>0
            return element_types[element_type]
        def expected_conditions(condition,*params):
            global EC
            from selenium.webdriver.support import expected_conditions as EC
            expected_conditions     =  {"title_is"                                  :   EC.title_is,
                                        "title_contains"                            :   EC.title_contains,
                                        "presence_of_element_located"               :   EC.presence_of_element_located,
                                        "visibility_of_element_located"             :   EC.visibility_of_element_located,
                                        "visibility_of"                             :   EC.visibility_of,
                                        "presence_of_all_elements_located"          :   EC.presence_of_all_elements_located,
                                        "text_to_be_present_in_element"             :   EC.text_to_be_present_in_element,
                                        "text_to_be_present_in_element_value"       :   EC.text_to_be_present_in_element_value,
                                        "frame_to_be_available_and_switch_to_it"    :   EC.frame_to_be_available_and_switch_to_it,
                                        "invisibility_of_element_located"           :   EC.invisibility_of_element_located,
                                        "element_to_be_clickable"                   :   EC.element_to_be_clickable,
                                        "staleness_of"                              :   EC.staleness_of,
                                        "element_to_be_selected"                    :   EC.element_to_be_selected,
                                        "element_located_to_be_selected"            :   EC.element_located_to_be_selected,
                                        "element_selection_state_to_be"             :   EC.element_selection_state_to_be,
                                        "element_located_selection_state_to_be"     :   EC.element_located_selection_state_to_be,
                                        "alert_is_present"                          :   EC.alert_is_present}
            assert expected_conditions.keys().count(condition)>0
            if condition=='presence_of_element_located':
                expected_res        =   (element_types(params[0]), params[1])
                condition_res       =   expected_conditions[condition](expected_res)
            else:
                assert True=='DEVELOPMENT INCOMPLETE'
            return condition_res


        global WebDriverWait
        from selenium.webdriver.support.ui import WebDriverWait


        if not invert:
            WebDriverWait(self.window, timeout_seconds,poll_frequency).until(
                expected_conditions(condition,param1,param2))
        else:
            WebDriverWait(self.window, timeout_seconds,poll_frequency).until_not(
                expected_conditions(condition,param1,param2))

    def wait_for_page(self,timeout_seconds=45):
        """
        Include start page to confirm change started ??
        """
        end                     =   TIME() + timeout_seconds
        delay(                      1)
        while TIME()<end:
            status              =   self.window.execute_script("return document.readyState")
            if status=='complete':
                return
            else:
                delay(              2)

    def window_count(self):
        return len(self.window.window_handles)

    def zoom(self,percentage):
        self.window.execute_script('document.body.style.zoom = "%s";' % percentage)

    def misc_code():
        #     jcode="document.title;"
        #     a=br.execute_script(jcode)
        #     br.find_element_by_name("uid").send_keys(usr)
        #     br.find_element_by_name("upass").send_keys(pw)
        #     br.find_element_by_name("continueButtonExisting").click()
        #     br.find_element_by_xpath("//a[contains(@href,'"+it+"')]").click()  
        #     br.get_window_size('current')
        #     br.switch_to_window(br.window_handles[0])
        pass

class Urllib2:

    def __init__(self,browser,cookies):
        import urllib2
        self.browser                        =   urllib2
        if cookies != '':
            cj                              =   set_cookies_from_text(cookies)
            opener                          =   urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
            self.browser.install_opener(        opener)
        
    def get_page(self, gotoUrl):
        return self.browser.urlopen(gotoUrl, data=None,timeout=3600)

class scraper:

    def __init__(self,browser=None,**kwargs):
        if kwargs.has_key('dict'):              # e.g., scraper('phantom',{'dict':self.T}).browser
            self.T                          =   kwargs['dict']
            del kwargs['dict']
        if kwargs:
            self.kwargs                     =   kwargs

        if browser == 'mechanize':
            t                               =   Mechanize(self)
            self.browser,self.browserType   =   t.browser,t.browserType
        
        if ['firefox','phantom','chrome'].count(browser):
            self.browser                    =   Webdriver(self,browser)
            self.browser.update_cookies(        )
        
        if browser == 'urllib2': 
            self.browser                    =   Urllib2(self)