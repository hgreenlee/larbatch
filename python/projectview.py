#! /usr/bin/env python
######################################################################
#
# Name: projectview.py
#
# Purpose: Python class ProjectView is a widget for displaying 
#          information about projects.
#
# Created: 21-Jan-2015  Herbert Greenlee
#
######################################################################

# Import GUI stuff

from Tkinter import *

# Project widget class

class ProjectView(Frame):

    # Constructor.

    def __init__(self, parent, xml_file=None):

        self.root = parent

        # Register our outermost frame in the parent window.

        Frame.__init__(self, self.root)
        self.pack(expand=1, fill=BOTH)

        # Make widgets that belong to this widget.

        self.make_widgets()
        self.set_xml_file(xml_file)
       

    # Make widgets.  Make and register all widgets in the application window.

    def make_widgets(self):

        # Add a button (testing).

        self.label = Label(self, bd=2, relief=RAISED)
        self.label.pack(side=BOTTOM, expand=1, fill=BOTH)

    # Set the name of the xml file.

    def set_xml_file(self, xml_file):
        self.xml_file = xml_file
        self.label['text'] = self.xml_file
        

