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

import Tkinter as tk
from statusview import ProjectStatusView

# Project widget class

class ProjectView(tk.Frame):

    # Constructor.

    def __init__(self, parent, project_name=None, xml_path=None, project_def=None):

        self.root = parent

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.root)
        self.pack(expand=1, fill=tk.BOTH)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

        # Make widgets that belong to this widget.

        self.make_widgets()
        self.set_project(project_name, xml_path, project_def)       

    # Make widgets.

    def make_widgets(self):

        # Add a label which will display the current project name.

        self.label = tk.Label(self, relief=tk.SUNKEN)
        self.label.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W)

        # Add a scrolling text box for displaying xml.
        # Make scroll bars, but don't grid them yet.

        self.xmlframe = tk.Frame(self)
        #self.xmlframe.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.xmlframe.rowconfigure(0, weight=1)
        self.xmlframe.columnconfigure(0, weight=1)
        self.xml = tk.Text(self.xmlframe, height=24, width=80, wrap=tk.NONE)
        self.xml.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.xmlvbar = tk.Scrollbar(self.xmlframe, orient=tk.VERTICAL, command=self.xml.yview)
        self.xml['yscrollcommand'] = self.xmlvbar.set
        self.xmlvbar_visible = 0
        self.xmlhbar = tk.Scrollbar(self.xmlframe, orient=tk.HORIZONTAL, command=self.xml.xview)
        self.xml['xscrollcommand'] = self.xmlhbar.set
        self.xmlhbar_visible = 0
        self.check_scroll()
 
	# Set event bindings (check scrollbar visibility when window size 
        # changes).

	self.xml.bind('<Configure>', self.check_scroll)
        self.xml['state'] = tk.DISABLED

        # Add a project status view.
       
        # Add a frame for project status view.

        self.ps = ProjectStatusView(self)
        self.ps.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W+tk.S)

    # Set the name of the xml file.

    def set_project(self, project_name, xml_path, project_def):
        self.label['text'] = project_name
        if xml_path != None:
            f = open(xml_path)
            xmltext = f.read()
            self.xml['state'] = tk.NORMAL
            self.xml.delete('1.0', tk.END)
            self.xml.insert('1.0', xmltext)
            self.xml['state'] = tk.DISABLED
        if project_def != None:
            self.ps.set_project(project_def)
        
    # See if we need to enable or disable either scrollbar.

    def check_scroll(self, event=None):
	yv = self.xml.yview()
	if not self.xmlvbar_visible and (yv[0] != 0.0 or yv[1] != 1.0):
	    self.xmlvbar_visible = 1
	    self.xmlvbar.grid(row=0, column=1, sticky=tk.N+tk.S)
	elif self.xmlvbar_visible and yv[0] == 0.0 and yv[1] == 1.0:
	    self.xmlvbar_visible = 0
	    self.xmlvbar.grid_forget()
	xv = self.xml.xview()
	if not self.xmlhbar_visible and (xv[0] != 0.0 or xv[1] != 1.0):
	    self.xmlhbar_visible = 1
	    self.xmlhbar.grid(row=1, column=0, sticky=tk.E+tk.W)
	elif self.xmlhbar_visible and xv[0] == 0.0 and xv[1] == 1.0:
	    self.xmlhbar_visible = 0
	    self.xmlhbar.grid_forget()

    # Set XML view.

    def xml_view(self):
        self.xmlframe.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.ps.grid_forget()

    # Set project status view.

    def project_status_view(self):
        self.xmlframe.grid_forget()
        self.ps.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
