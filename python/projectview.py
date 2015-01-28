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
import tkMessageBox
from statusview import ProjectStatusView
from textwindow import TextWindow

# Project widget class

class ProjectView(tk.Frame):

    # Constructor.

    def __init__(self, parent, project_name=None, xml_path=None, project_def=None):

        self.parent = parent

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.parent)
        self.pack(expand=1, fill=tk.BOTH)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

        # Make widgets that belong to this widget.

        self.make_widgets()
        self.set_project(project_name, xml_path, project_def)       

    # Make widgets.

    def make_widgets(self):

        # Add a label which will display the current project xml file path.

        self.path = tk.Label(self, relief=tk.SUNKEN)
        self.path.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W)

        # Add a label which will display the current project name.

        self.label = tk.Label(self, relief=tk.SUNKEN)
        self.label.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W)

        # Add a project status view.

        self.ps = ProjectStatusView(self)
        self.ps.grid(row=2, column=0, sticky=tk.N+tk.E+tk.W+tk.S)

    # Select the project.

    def set_project(self, project_name, xml_path, project_def):
        self.path['text'] = xml_path
        self.label['text'] = project_name
        if project_def != None:
            self.ps.set_project(project_def)

    # Make a top level window for displaying xml.

    def make_xml_window(self):

        xml_path = self.path['text']
        if xml_path == None or xml_path == '':
            tkMessageBox.showerror('', 'No xml file specified.')
            return

        # Get text of xml file.

        f = open(xml_path)
        if not f:
            tkMessageBox.showerror('', 'Error opening xml file %s.' % xml_path)
            return
        xmltext = f.read()

        # Make a new top level window to hold xml text.
        # After we are done making this window, we don't keep track
        # of it any more.  It is owned by window manager.

        w = TextWindow()
        w.insert(tk.END, xmltext)

    # Update status view.

    def update_status(self):
        self.ps.update_status()

    # Update bach jobs.

    def update_jobs(self):
        self.ps.update_jobs()

    # Highlight stage.

    def highlight_stage(self, stagename):
        self.ps.highlight_stage(stagename)
