#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2012-11-02 11:16:02

import cgi
import re
import six
import json
import chardet
import lxml.html
import lxml.etree
from tblib import Traceback
from pyquery import PyQuery
from requests.structures import CaseInsensitiveDict
from requests import HTTPError
from pyspider.libs import utils
import logging

logger = logging.getLogger('response')
class Response(object):

    def __init__(self, status_code=None, url=None, orig_url=None, headers=CaseInsensitiveDict(),
                 content='', cookies=None, error=None, traceback=None, save=None, js_script_result=None, time=0, 
                 page_num = None, project_name=None, configure={}, sequence=None, group=None):
        if cookies is None:
            cookies = {}
        self.status_code = status_code
        self.url = url
        self.orig_url = orig_url
        self.headers = headers
        self.content = content
        self.cookies = cookies
        self.error = error
        self.traceback = traceback
        self.save = save
        self.js_script_result = js_script_result
        self.time = time
        self._page_num = page_num
        self.project_name = project_name
        self.configure = configure
        self._sequence = sequence
        self._group = group

    def __repr__(self):
        return u'<Response [%d]>' % self.status_code

    def __bool__(self):
        """Returns true if `status_code` is 200 and no error"""
        return self.ok

    def __nonzero__(self):
        """Returns true if `status_code` is 200 and no error."""
        return self.ok

    def set_sequence(self, value):
        self._sequence = value
    @property
    def ok(self):
        """Return true if `status_code` is 200 and no error."""
        try:
            self.raise_for_status()
        except:
            return False
        return True

    @property
    def page_num(self):
        return self._page_num

    @property
    def group(self):
        return self._group

    @property
    def encoding(self):
        """
        encoding of Response.content.

        if Response.encoding is None, encoding will be guessed
        by header or content or chardet if available.
        """
        if hasattr(self, '_encoding'):
            return self._encoding

        # content is unicode
        if isinstance(self.content, six.text_type):
            return 'unicode'

        # Try charset from content-type or content
        encoding = get_encoding(self.headers, self.content)

        # Fallback to auto-detected encoding.
        if not encoding and chardet is not None:
            encoding = chardet.detect(self.content[:600])['encoding']

        if encoding and encoding.lower() == 'gb2312':
            encoding = 'gb18030'

        self._encoding = encoding or 'utf-8'
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        """
        set encoding of content manually
        it will overwrite the guessed encoding
        """
        self._encoding = value
        self._text = None

    @property
    def project(self):
        return self.project_name

    @property
    def config(self):
        return self.configure

    @property
    def sequence(self):
        return self._sequence

    @property
    def text(self):
        """
        Content of the response, in unicode.

        if Response.encoding is None and chardet module is available, encoding
        will be guessed.
        """
        if hasattr(self, '_text') and self._text:
            return self._text
        if not self.content:
            return u''
        if isinstance(self.content, six.text_type):
            return self.content

        content = None
        encoding = self.encoding

        # Decode unicode from given encoding.
        try:
            content = self.content.decode(encoding, 'replace')
        except LookupError:
            # A LookupError is raised if the encoding was not found which could
            # indicate a misspelling or similar mistake.
            #
            # So we try blindly encoding.
            content = self.content.decode('utf-8', 'replace')

        self._text = content
        return content

    @property
    def json(self):
        """Returns the json-encoded content of the response, if any."""
        if hasattr(self, '_json'):
            return self._json
        try:
            self._json = json.loads(self.text or self.content)
        except ValueError:
            self._json = None
        return self._json

    @property
    def fulldoc(self):
        """Returns a PyQuery object of the response's text"""
        if hasattr(self, '_fulldoc'):
            return self._fulldoc
        elements = self.fulletree
        doc = self._fulldoc = PyQuery(elements)
        doc.make_links_absolute(utils.text(self.url))
        return doc

    @property
    def fulletree(self):
        """Returns a lxml object of the response's text that can be selected by xpath"""
        if not hasattr(self, '_fullelements'):
            try:
                parser = lxml.html.HTMLParser(encoding=self.encoding)
                text = "<html>" + re.sub('</?html[\s\S]*?>', '', self.text) + "</html>"
                self._fullelements = lxml.html.fromstring(text, parser=parser)
            except LookupError:
                # lxml would raise LookupError when encoding not supported
                # try fromstring without encoding instead.
                # on windows, unicode is not availabe as encoding for lxml
                self._fullelements = lxml.html.fromstring(self.text)
        if isinstance(self._fullelements, lxml.etree._ElementTree):
            self._fullelements = self._fullelements.getroot()
        return self._fullelements

    @property
    def doc(self):
        """Returns a PyQuery object of the response's content"""
        if hasattr(self, '_doc'):
            return self._doc
        elements = self.etree
        doc = self._doc = PyQuery(elements)
        doc.make_links_absolute(utils.text(self.url))
        return doc

    @property
    def etree(self):
        """Returns a lxml object of the response's content that can be selected by xpath"""
        if not hasattr(self, '_elements'):
            try:
                logger.error('encoding is %s'%(self.encoding))
                parser = lxml.html.HTMLParser(encoding=self.encoding)
                self._elements = lxml.html.fromstring(self.content, parser=parser)
            except LookupError:
                # lxml would raise LookupError when encoding not supported
                # try fromstring without encoding instead.
                # on windows, unicode is not availabe as encoding for lxml
                logger.error('lxml is error when content is %s'%(self.content))
                self._elements = lxml.html.fromstring(self.content)
        if isinstance(self._elements, lxml.etree._ElementTree):
            self._elements = self._elements.getroot()
        return self._elements

    def raise_for_status(self, allow_redirects=True):
        """Raises stored :class:`HTTPError` or :class:`URLError`, if one occurred."""

        if self.status_code == 304:
            return
        elif self.error:
            if self.traceback:
                six.reraise(Exception, Exception(self.error), Traceback.from_string(self.traceback).as_traceback())
            http_error = HTTPError(self.error)
        elif (self.status_code >= 300) and (self.status_code < 400) and not allow_redirects:
            http_error = HTTPError('%s Redirection' % (self.status_code))
        elif (self.status_code >= 400) and (self.status_code < 500):
            http_error = HTTPError('%s Client Error' % (self.status_code))
        elif (self.status_code >= 500) and (self.status_code < 600):
            http_error = HTTPError('%s Server Error' % (self.status_code))
        else:
            return

        http_error.response = self
        raise http_error

    def isok(self):
        try:
            self.raise_for_status()
            return True
        except:
            return False


def rebuild_response(r):
    response = Response(
        status_code=r.get('status_code', 599),
        url=r.get('url', ''),
        headers=CaseInsensitiveDict(r.get('headers', {})),
        content=r.get('content', ''),
        cookies=r.get('cookies', {}),
        error=r.get('error'),
        traceback=r.get('traceback'),
        time=r.get('time', 0),
        orig_url=r.get('orig_url', r.get('url', '')),
        js_script_result=r.get('js_script_result'),
        save=r.get('save'),
        project_name=r.get('project_name'),
        configure=r.get('configure'),
        sequence=r.get('sequence'),
        page_num=r.get('page_num'),
        group=r.get('group')
    )
    return response


def get_encoding(headers, content):
    """Get encoding from request headers or page head."""
    encoding = None

    content_type = headers.get('content-type')
    if content_type:
        _, params = cgi.parse_header(content_type)
        if 'charset' in params:
            encoding = params['charset'].strip("'\"")

    if not encoding:
        content = utils.pretty_unicode(content[:1000]) if six.PY3 else content

        charset_re = re.compile(r'<meta.*?charset=["\']*(.+?)["\'>]',
                                flags=re.I)
        pragma_re = re.compile(r'<meta.*?content=["\']*;?charset=(.+?)["\'>]',
                               flags=re.I)
        xml_re = re.compile(r'^<\?xml.*?encoding=["\']*(.+?)["\'>]')
        encoding = (charset_re.findall(content) +
                    pragma_re.findall(content) +
                    xml_re.findall(content))
        encoding = encoding and encoding[0] or None

    return encoding
