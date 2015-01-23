#! /usr/bin/env python
######################################################################
#
# Name: statusview.py
#
# Purpose: Python class StatusView is a widget for displaying 
#          project status information.
#
# Created: 22-Jan-2015  Herbert Greenlee
#
######################################################################

from projectstatus import ProjectStatus

# Import GUI stuff

import Tkinter as tk

# Make symbolic names for column indices.

kEXISTS = 1
kNFILE = 2
kNEV = 3
kNERROR = 4
kNMISS = 5

# Project widget class

class ProjectStatusView(tk.Frame):

    # Constructor.

    def __init__(self, parent, project_def=None):

        self.root = parent
        self.project_def = project_def

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.root)
        self.pack(expand=1, fill=tk.BOTH)
        self.rowconfigure(0, weight=1)

        # Dictionaries to hold stage widgets.

        self.stage_name_labels = {}
        self.exists_labels = {}
        self.nfile_labels = {}
        self.nev_labels = {}
        self.nerror_labels = {}
        self.nmiss_labels = {}

        # Make widgets that belong to this widget.

        self.make_widgets()
        if self.project_def != None:
            self.update_status()

    # Make widgets.

    def make_widgets(self):

        # Add column headings.

        self.stage_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Stage')
        self.stage_head.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(0, weight=1)
        self.exists_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Exists?')
        self.exists_head.grid(row=0, column=kEXISTS, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kEXISTS, weight=1)
        self.nfile_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Files')
        self.nfile_head.grid(row=0, column=kNFILE, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNFILE, weight=1)
        self.nev_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Events')
        self.nev_head.grid(row=0, column=kNEV, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNEV, weight=1)
        self.nerror_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Errors')
        self.nerror_head.grid(row=0, column=kNERROR, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNERROR, weight=1)
        self.miss_head = tk.Label(self, bg='white', relief=tk.RIDGE, text='Missing')
        self.miss_head.grid(row=0, column=kNMISS, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNMISS, weight=1)

    # Set or update project definition.

    def set_project(self, project_def):
        self.project_def = project_def
        self.update_status()

    # Update status display.

    def update_status(self):

        print 'Updating status.'
        ps = ProjectStatus(self.project_def)
        print 'Done.'

        # Update label widgets.

        for key in self.stage_name_labels.keys():
            self.stage_name_labels[key].grid_forget()
        for key in self.exists_labels.keys():
            self.exists_labels[key].grid_forget()
        for key in self.nfile_labels.keys():
            self.nfile_labels[key].grid_forget()
        for key in self.nev_labels.keys():
            self.nev_labels[key].grid_forget()
        for key in self.nerror_labels.keys():
            self.nerror_labels[key].grid_forget()
        for key in self.nmiss_labels.keys():
            self.nmiss_labels[key].grid_forget()

        row = 0
        for stage in self.project_def.stages:
            row = row + 1
            ss = ps.get_stage_status(stage.name)

            if not self.stage_name_labels.has_key(stage.name):
                self.stage_name_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            self.stage_name_labels[stage.name]['text'] = stage.name
            self.stage_name_labels[stage.name].grid(row=row, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.exists_labels.has_key(stage.name):
                self.exists_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            if ss.exists:
                self.exists_labels[stage.name]['text'] = 'Yes'
            else:
                self.exists_labels[stage.name]['text'] = 'No'
            self.exists_labels[stage.name].grid(row=row, column=kEXISTS, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nfile_labels.has_key(stage.name):
                self.nfile_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            self.nfile_labels[stage.name]['text'] = str(ss.nfile)
            self.nfile_labels[stage.name].grid(row=row, column=kNFILE, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nev_labels.has_key(stage.name):
                self.nev_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            self.nev_labels[stage.name]['text'] = str(ss.nev)
            self.nev_labels[stage.name].grid(row=row, column=kNEV, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nerror_labels.has_key(stage.name):
                self.nerror_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            self.nerror_labels[stage.name]['text'] = str(ss.nerror)
            self.nerror_labels[stage.name].grid(row=row, column=kNERROR, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nmiss_labels.has_key(stage.name):
                self.nmiss_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE)
            self.nmiss_labels[stage.name]['text'] = str(ss.nmiss)
            self.nmiss_labels[stage.name].grid(row=row, column=kNMISS, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

        
