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

import sys
from projectstatus import ProjectStatus
from batchstatus import BatchStatus

# Import GUI stuff

import Tkinter as tk
import tkMessageBox
import tkFont

# Make symbolic names for column indices.

kEXISTS = 1
kNFILE = 2
kNEV = 3
kNERROR = 4
kNMISS = 5
kIDLE = 6
kRUNNING = 7
kHELD = 8
kOTHER = 9

# Project widget class

class ProjectStatusView(tk.Frame):

    # Constructor.

    def __init__(self, parent, project_def=None):


        self.parent = parent
        self.project_def = project_def

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.parent)
        self.pack(expand=1, fill=tk.BOTH)
        self.rowconfigure(0, weight=1)

        # Dictionaries to hold stage widgets.

        self.stage_name_labels = {}
        self.exists_labels = {}
        self.nfile_labels = {}
        self.nev_labels = {}
        self.nerror_labels = {}
        self.nmiss_labels = {}
        self.nidle_labels = {}
        self.nrunning_labels = {}
        self.nheld_labels = {}
        self.nother_labels = {}

        # Make widgets that belong to this widget.

        self.make_widgets()
        if self.project_def != None:
            self.update_status()

    # Make widgets.

    def make_widgets(self):

        # Add column category headings.

        self.stage_cat = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='',
                                  padx=10, pady=10, font=tkFont.Font(size=12))
        self.stage_cat.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.files_cat = tk.Frame(self, bd=1, relief=tk.SUNKEN)
        files_label = tk.Label(self.files_cat, bg='lightgray', relief=tk.FLAT, text='Files',
                                  padx=10, font=tkFont.Font(size=12))
        files_label.pack(side=tk.LEFT, expand=1, fill=tk.BOTH)
        checkana_button = tk.Button(self.files_cat, bg='lightgray', text='CheckAna',
                                    command=self.parent.parent.checkana)
        checkana_button.pack(side=tk.RIGHT)
        check_button = tk.Button(self.files_cat, bg='lightgray', text='Check',
                                 command=self.parent.parent.check)
        check_button.pack(side=tk.RIGHT)
        self.files_cat.grid(row=0, column=kEXISTS, columnspan=5, sticky=tk.N+tk.E+tk.W+tk.S)
        self.batch_cat = tk.Frame(self, bd=1, relief=tk.SUNKEN)
        batch_label = tk.Label(self.batch_cat, bg='lightgray', relief=tk.FLAT, text='Batch Jobs',
                               padx=10, font=tkFont.Font(size=12))
        batch_label.pack(side=tk.LEFT, expand=1, fill=tk.BOTH)
        batch_button = tk.Button(self.batch_cat, bg='lightgray', text='Update',
                                 command=self.update_jobs)
        batch_button.pack(side=tk.RIGHT)
        self.batch_cat.grid(row=0, column=kIDLE, columnspan=4, sticky=tk.N+tk.E+tk.W+tk.S)

        # Add column headings.

        self.stage_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Stage',
                                   padx=10, pady=10, font=tkFont.Font(size=12))
        self.stage_head.grid(row=1, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(0, weight=1)
        self.exists_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Exists?',
                                    padx=10, font=tkFont.Font(size=12))
        self.exists_head.grid(row=1, column=kEXISTS, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kEXISTS, weight=1)
        self.nfile_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Files',
                                   padx=10, font=tkFont.Font(size=12))
        self.nfile_head.grid(row=1, column=kNFILE, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNFILE, weight=1)
        self.nev_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Events',
                                 padx=10, font=tkFont.Font(size=12))
        self.nev_head.grid(row=1, column=kNEV, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNEV, weight=1)
        self.nerror_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Errors',
                                    padx=10, font=tkFont.Font(size=12))
        self.nerror_head.grid(row=1, column=kNERROR, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNERROR, weight=1)
        self.miss_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Missing',
                                  padx=10, font=tkFont.Font(size=12))
        self.miss_head.grid(row=1, column=kNMISS, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kNMISS, weight=1)
        self.idle_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Idle',
                                  padx=10, font=tkFont.Font(size=12))
        self.idle_head.grid(row=1, column=kIDLE, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kIDLE, weight=1)
        self.running_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Running',
                                     padx=10, font=tkFont.Font(size=12))
        self.running_head.grid(row=1, column=kRUNNING, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kRUNNING, weight=1)
        self.held_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Held',
                                  padx=10, font=tkFont.Font(size=12))
        self.held_head.grid(row=1, column=kHELD, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kHELD, weight=1)
        self.other_head = tk.Label(self, bg='lightgray', relief=tk.RIDGE, text='Other',
                                   padx=10, font=tkFont.Font(size=12))
        self.other_head.grid(row=1, column=kOTHER, sticky=tk.N+tk.E+tk.W+tk.S)
        self.columnconfigure(kOTHER, weight=1)

    # Set or update project definition.

    def set_project(self, project_def):
        self.project_def = project_def
        self.update_status()

    # Update status display.

    def update_status(self):

        try:
            ps = ProjectStatus(self.project_def)
            bs = BatchStatus(self.project_def)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])

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
        for key in self.nidle_labels.keys():
            self.nidle_labels[key].grid_forget()
        for key in self.nrunning_labels.keys():
            self.nrunning_labels[key].grid_forget()
        for key in self.nheld_labels.keys():
            self.nheld_labels[key].grid_forget()
        for key in self.nother_labels.keys():
            self.nother_labels[key].grid_forget()

        row = 1
        for stage in self.project_def.stages:
            row = row + 1
            ss = ps.get_stage_status(stage.name)
            bss = bs.get_stage_status(stage.name)

            if not self.stage_name_labels.has_key(stage.name):
                self.stage_name_labels[stage.name] = tk.Label(self, bg='lightgray', relief=tk.RIDGE,
                                                              padx=10, pady=5,
                                                              font=tkFont.Font(size=12))
            self.stage_name_labels[stage.name]['text'] = stage.name
            self.stage_name_labels[stage.name].bind('<Button-1>', self.click_stage)
            self.stage_name_labels[stage.name].grid(row=row, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.exists_labels.has_key(stage.name):
                self.exists_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                          font=tkFont.Font(size=12))
            if ss.exists:
                self.exists_labels[stage.name]['text'] = 'Yes'
                self.exists_labels[stage.name]['fg'] = 'black'
            else:
                self.exists_labels[stage.name]['text'] = 'No'
                self.exists_labels[stage.name]['fg'] = 'red'
            self.exists_labels[stage.name].grid(row=row, column=kEXISTS, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nfile_labels.has_key(stage.name):
                self.nfile_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                         font=tkFont.Font(size=12))
            self.nfile_labels[stage.name]['text'] = str(ss.nfile)
            if ss.nfile == 0:
                self.nfile_labels[stage.name]['fg'] = 'red'
            else:
                self.nfile_labels[stage.name]['fg'] = 'black'
            self.nfile_labels[stage.name].grid(row=row, column=kNFILE, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nev_labels.has_key(stage.name):
                self.nev_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                       font=tkFont.Font(size=12))
            self.nev_labels[stage.name]['text'] = str(ss.nev)
            if ss.nev == 0:
                self.nev_labels[stage.name]['fg'] = 'red'
            else:
                self.nev_labels[stage.name]['fg'] = 'black'
            self.nev_labels[stage.name].grid(row=row, column=kNEV, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nerror_labels.has_key(stage.name):
                self.nerror_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                          font=tkFont.Font(size=12))
            self.nerror_labels[stage.name]['text'] = str(ss.nerror)
            if ss.nerror == 0:
                self.nerror_labels[stage.name]['fg'] = 'black'
            else:
                self.nerror_labels[stage.name]['fg'] = 'red'
            self.nerror_labels[stage.name].grid(row=row, column=kNERROR, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nmiss_labels.has_key(stage.name):
                self.nmiss_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                         font=tkFont.Font(size=12))
            self.nmiss_labels[stage.name]['text'] = str(ss.nmiss)
            if ss.nmiss == 0:
                self.nmiss_labels[stage.name]['fg'] = 'black'
            else:
                self.nmiss_labels[stage.name]['fg'] = 'red'
            self.nmiss_labels[stage.name].grid(row=row, column=kNMISS, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nidle_labels.has_key(stage.name):
                self.nidle_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                         font=tkFont.Font(size=12))
            self.nidle_labels[stage.name]['text'] = bss[0]
            self.nidle_labels[stage.name].grid(row=row, column=kIDLE, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nrunning_labels.has_key(stage.name):
                self.nrunning_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                            font=tkFont.Font(size=12))
            self.nrunning_labels[stage.name]['text'] = bss[1]
            self.nrunning_labels[stage.name].grid(row=row,
                                                  column=kRUNNING, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nheld_labels.has_key(stage.name):
                self.nheld_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                         font=tkFont.Font(size=12))
            self.nheld_labels[stage.name]['text'] = bss[2]
            self.nheld_labels[stage.name].grid(row=row, column=kHELD, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

            if not self.nother_labels.has_key(stage.name):
                self.nother_labels[stage.name] = tk.Label(self, bg='white', relief=tk.RIDGE,
                                                          font=tkFont.Font(size=12))
            self.nother_labels[stage.name]['text'] = bss[3]
            self.nother_labels[stage.name].grid(row=row, column=kOTHER, sticky=tk.N+tk.E+tk.W+tk.S)
            self.rowconfigure(row, weight=1)

        
    # Highlight stage.

    def highlight_stage(self, stagename):
        for key in self.stage_name_labels.keys():
            self.stage_name_labels[key]['bg'] = 'lightgray'
        if self.stage_name_labels.has_key(stagename):
            self.stage_name_labels[stagename]['bg'] = 'white'

    # Update jobs button.

    def update_jobs(self):
        try:
            BatchStatus.update_jobs()
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        self.update_status()
        
    # Click on stage mouse event callback.

    def click_stage(self, event):
        stagename = event.widget['text']

        # Choose stage in parent app.

        self.parent.parent.choose_stage(stagename)
