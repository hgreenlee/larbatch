#! /usr/bin/env python
######################################################################
#
# Name: projectapp.py
#
# Purpose: Python class ProjectApp for project.py gui interface.
#
# Created: 16-Jan-2015  Herbert Greenlee
#
######################################################################

# Standard imports

import sys, os, subprocess, traceback
from sets import Set

# Import project.py as a module.

import project
import project_utilities
from batchstatus import BatchStatus

# Import Tkinter GUI stuff

import Tkinter as tk
#import ttk
import tkFileDialog
import tkMessageBox
import tkFont
from projectview import ProjectView
from textwindow import TextWindow

ticket_ok = False

# Main window class for this gui application.

class ProjectApp(tk.Frame):

    # Constructor.

    def __init__(self, parent=None):

        # Root window.

        if parent == None:
            self.root = tk.Tk()
        else:
            self.root = parent
        self.root.title('project')
        self.root.protocol('WM_DELETE_WINDOW', self.close)

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.root)
        self.pack(expand=1, fill=tk.BOTH)

        # Make widgets that belong to this app.

        self.make_widgets()

        # Current project and stage (menu settable).

        self.current_project_name = ''
        self.current_project_def = None
        self.current_stage_name = ''
        self.current_stage_def = None

        # Known projects (3-tuple: project_name, xml_path, project_def)

        self.projects = []

        # Process command line arguments.

        for n in range(1, len(sys.argv)):
            xmlpath = sys.argv[n]
            self.open(xmlpath, choose=False)
        if len(self.projects) > 0:
            self.choose_project(self.projects[0][0])

    # Make widgets.  Make and register all widgets in the application window.

    def make_widgets(self):

        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)

        # Make menu bar (row 0).

        self.make_menubar()

        # Add project view widget (row 1).

        self.project_view = ProjectView(self)
        self.project_view.grid(row=1, column=0, sticky=tk.E+tk.W)

        # Add console window widget (row 2).

        self.console = TextWindow(self)
        self.console.grid(row=2, column=0, sticky=tk.N+tk.E+tk.W+tk.S)

        # From now on, send standard and diagnostic output to console window.

        sys.stdout = self.console
        sys.stderr = self.console

    # Make a menubar widget.

    def make_menubar(self):

        # Put menu in its own frame.

        self.menubar = tk.Frame(self)
        self.menubar.grid(row=0, column=0, sticky=tk.E+tk.W)

        # File menu.

        mbutton = tk.Menubutton(self.menubar, text='File', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        file_menu = tk.Menu(mbutton)
        file_menu.add_command(label='Open Project', command=self.open)
        file_menu.add_command(label='Quit', command=self.close)
        mbutton['menu'] = file_menu

        # View menu.

        mbutton = tk.Menubutton(self.menubar, text='View', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        view_menu = tk.Menu(mbutton)
        view_menu.add_command(label='XML', command=self.xml_view)
        mbutton['menu'] = view_menu

        # Project menu.

        mbutton = tk.Menubutton(self.menubar, text='Project', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.project_menu = tk.Menu(mbutton)
        self.project_menu.add_command(label='Next Project', command=self.next_project,
                                      accelerator='PgUp')
        self.project_menu.add_command(label='Previous Project', command=self.previous_project,
                                      accelerator='PgDn')
        self.project_menu.add_separator()
        mbutton['menu'] = self.project_menu

        # Add project menu key bindings.

        self.project_menu.bind_all('<KeyPress-KP_Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-KP_Prior>', self.previous_project_handler)
        self.project_menu.bind_all('<KeyPress-Prior>', self.previous_project_handler)

        # Stage menu.

        mbutton = tk.Menubutton(self.menubar, text='Stage', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.stage_menu = tk.Menu(mbutton)
        self.stage_menu.add_command(label='Next Stage', command=self.next_stage,
                                    accelerator='Up')
        self.stage_menu.add_command(label='Previous Stage', command=self.previous_stage,
                                    accelerator='Down')
        self.stage_menu.add_separator()
        mbutton['menu'] = self.stage_menu

        # Add stage menu key bindings.

        self.stage_menu.bind_all('<KeyPress-KP_Up>', self.previous_stage_handler)
        self.stage_menu.bind_all('<KeyPress-Up>', self.previous_stage_handler)
        self.stage_menu.bind_all('<KeyPress-KP_Down>', self.next_stage_handler)
        self.stage_menu.bind_all('<KeyPress-Down>', self.next_stage_handler)

        # Output menu.

        mbutton = tk.Menubutton(self.menubar, text='Output', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.output_menu = tk.Menu(mbutton)
        self.output_menu.add_command(label='Check', command=self.check)
        self.output_menu.add_command(label='Checkana', command=self.checkana)
        self.output_menu.add_command(label='Fetchlog', command=self.fetchlog)
        self.output_menu.add_separator()
        self.output_menu.add_command(label='Histogram merge', command=self.mergehist)
        self.output_menu.add_command(label='Ntuple merge', command=self.mergentuple)
        self.output_menu.add_command(label='Custom merge', command=self.merge)
        self.output_menu.add_separator()
        self.output_menu.add_command(label='Clean', command=self.clean)
        mbutton['menu'] = self.output_menu

        # Batch menu.

        mbutton = tk.Menubutton(self.menubar, text='Batch', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.batch_menu = tk.Menu(mbutton)
        self.batch_menu.add_command(label='Submit', command=self.submit)
        self.batch_menu.add_command(label='Makeup', command=self.makeup)
        self.batch_menu.add_command(label='Update', command=self.update_jobs)
        self.batch_menu.add_command(label='Kill', command=self.kill_jobs)
        mbutton['menu'] = self.batch_menu

        # SAM menu.

        mbutton = tk.Menubutton(self.menubar, text='SAM', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.sam_menu = tk.Menu(mbutton)
        self.sam_menu.add_command(label='Check Declarations', command=self.check_declarations)
        self.sam_menu.add_command(label='Declare Files', command=self.declare)
        self.sam_menu.add_command(label='Test Declarations', command=self.test_declarations)
        self.sam_menu.add_separator()
        self.sam_menu.add_command(label='Check Dataset Definition', command=self.check_definition)
        self.sam_menu.add_command(label='Create Dataset Definition', command=self.define)
        self.sam_menu.add_command(label='Test Dataset Definition', command=self.test_definition)
        self.sam_menu.add_separator()
        self.sam_menu.add_command(label='Check Locations', command=self.check_locations)
        self.sam_menu.add_command(label='Add Disk Locations', command=self.add_locations)
        self.sam_menu.add_command(label='Clean Disk Locations', command=self.clean_locations)
        self.sam_menu.add_command(label='Remove Disk Locations', command=self.remove_locations)
        self.sam_menu.add_command(label='Upload to Enstore', command=self.upload)
        self.sam_menu.add_separator()
        self.sam_menu.add_command(label='Audit', command=self.audit)
        mbutton['menu'] = self.sam_menu

        # Help menu.

        mbutton = tk.Menubutton(self.menubar, text='Help', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.RIGHT)
        self.help_menu = tk.Menu(mbutton)
        self.help_menu.add_command(label='project.py help', command=self.help)
        self.help_menu.add_command(label='XML help', command=self.xmlhelp)
        mbutton['menu'] = self.help_menu

        return

    # Open project.

    def open(self, xml_path=None, choose=True):
        if xml_path == None:
            types = (('XML files', '*.xml'),
                     ('All files', '*'))
            d = tkFileDialog.Open(filetypes=types, parent=self.root)
            xml_path = d.show()

        # Parse xml into ProjectDef object, and from that extract project name.
        # This step can raise an exception for several reasons.  In that case, 
        # display a message and return without opening the file.

        try:
            project_def = project.ProjectDef(project.get_project(xml_path, ''))
        except:
            e = sys.exc_info()
            message = 'Error opening %s\n%s' % (xml_path, e[1])
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', message)
            return
            
        project_name = project_def.name

        # See if this project is already in the list of open projects.
        # If so, just choose this project, but don't open it again.

        for project_tuple in self.projects:
            if project_name == project_tuple[0]:
                if choose:
                    self.choose_project(project_name)
                return

        # Add this project to the list of known projects.

        self.projects.append((project_name, xml_path, project_def))

        # Update project menu.

        callback = tk._setit(tk.StringVar(), project_name, callback=self.choose_project)
        self.project_menu.add_command(label=project_name, command=callback)

        # Choose just-opened project.

        if choose:
            self.choose_project(project_name)

    # Choose already-opened project.

    def choose_project(self, value):
        self.current_project_name = ''
        self.current_project_def = None
        self.current_stage_name = ''
        self.current_stage_def = None
        self.project_view.highlight_stage(self.current_stage_name)
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            xml_path = project_tuple[1]
            project_def = project_tuple[2]
            if project_name == value:
                self.current_project_name = value
                self.current_project_def = project_def

                # Update project view widget.

                self.project_view.set_project(project_name, xml_path, project_def)

                # Update stage menu.

                self.stage_menu.delete(3, tk.END)
                self.stage_menu.add_separator()
                for stage in project_def.stages:
                    callback = tk._setit(tk.StringVar(), stage.name, callback=self.choose_stage)
                    self.stage_menu.add_command(label=stage.name, command=callback)

                return
    
        # It is an error if we fall out of the loop.

        raise 'No project: %s' % value

    # Choose stage.

    def choose_stage(self, value):
        if self.current_project_def != None:
            for stage in self.current_project_def.stages:
                if stage.name == value:
                    self.current_stage_name = value
                    self.current_stage_def = stage
                    self.project_view.highlight_stage(self.current_stage_name)

    # Key event handlers.

    def next_project_handler(self, event):
        self.next_project()
    def previous_project_handler(self, event):
        self.previous_project()
    def next_stage_handler(self, event):
        self.next_stage()
    def previous_stage_handler(self, event):
        self.previous_stage()
        
    # Select next project.
    def next_project(self):

        # Don't do anything if the project list is empty.

        if len(self.projects) == 0:
            return

        # Cycle through list of known projects.

        found = False
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            if found:
                self.choose_project(project_name)
                return
            if project_name == self.current_project_name:
                found = True

        # Choose first project if we fell out of the loop.

        self.choose_project(self.projects[0][0])

    # Select previous project.

    def previous_project(self):

        # Don't do anything if the project list is empty.

        if len(self.projects) == 0:
            return

        # Cycle through list of known projects.

        previous_project_name = self.projects[-1][0]
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            if project_name == self.current_project_name:
                self.choose_project(previous_project_name)
                return
            previous_project_name = project_name

        # Choose the last project if we fell out of the loop.

        self.choose_project(self.projects[-1][0])

    # Select XML view.

    def xml_view(self):
        self.project_view.make_xml_window()

    # Select next stage.

    def next_stage(self):
        if self.current_project_def != None:

            # Cycle through stages.

            found = False
            for stage in self.current_project_def.stages:
                if found:
                    self.choose_stage(stage.name)
                    return
                if stage.name == self.current_stage_name:
                    found = True

            # Choose first stage if we fell out of the loop.

            self.choose_stage(self.current_project_def.stages[0].name)

    # Select previous stage.

    def previous_stage(self):
        if self.current_project_def != None:

            # Cycle through stages.

            previous_stage_name = self.current_project_def.stages[-1].name
            for stage in self.current_project_def.stages:
                if stage.name == self.current_stage_name:
                    self.choose_stage(previous_stage_name)
                    return
                previous_stage_name = stage.name

            # Choose last stage if we fell out of the loop.

            self.choose_stage(self.current_project_def.stages[-1].name)

    # Check action.

    def check(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck(self.current_project_def, self.current_stage_def, ana=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Checkana action.

    def checkana(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck(self.current_project_def, self.current_stage_def, ana=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Fetchlog action.

    def fetchlog(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.dofetchlog(self.current_stage_def)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Clean action.

    def clean(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.doclean(self.current_project_def, self.current_stage_name)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Submit action.

    def submit(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return

        try:
            project.dosubmit(self.current_project_def, self.current_stage_def, makeup=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])
        BatchStatus.update_jobs()
        self.project_view.update_status()

    # Makeup action.

    def makeup(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return

        try:
            project.dosubmit(self.current_project_def, self.current_stage_def, makeup=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])
        BatchStatus.update_jobs()
        self.project_view.update_status()

    # Update jobs action.

    def update_jobs(self):
        self.project_view.update_jobs()

    # Kill jobs action.

    def kill_jobs(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return

        try:
            BatchStatus.update_jobs()
            jobs = BatchStatus.get_jobs()
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

        # Figure out which clusters to kill.
        
        cluster_ids = Set()
        for job in jobs:
            words = job.split()
            if len(words) >= 2:
                jobid = words[0]
                script = words[-1]
                workscript = '%s-%s.sh' % (self.current_stage_def.name,
                                           self.current_project_def.name)
                if script.find(workscript) == 0:
                    cp_server = jobid.split('@')
                    if len(cp_server) == 2:
                        clusproc = cp_server[0]
                        server = cp_server[1]
                        cp = clusproc.split('.')
                        if len(cp) == 2:
                            cluster = cp[0]
                            process = cp[1]
                            cluster_id = '%s@%s' % (cluster, server)
                            if not cluster_id in cluster_ids:
                                cluster_ids.add(cluster_id)

        # Actually issue kill commands.

        for cluster_id in cluster_ids:
            print 'Kill cluster id %s' % cluster_id
            command = ['jobsub_rm']
            command.append('--jobid=%s' % cluster_id)
            subprocess.call(command, stdout=sys.stdout, stderr=sys.stderr)

        self.update_jobs()
                           
        
    # Histogram merge.

    def mergehist(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.domerge(self.current_stage_def, mergehist=True, mergentuple=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Ntuple merge.

    def mergentuple(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.domerge(self.current_stage_def, mergehist=False, mergentuple=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Custom merge.

    def merge(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.domerge(self.current_stage_def, mergehist=False, mergentuple=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Declare files to sam.

    def declare(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck_declarations(self.current_stage_def.logdir, declare=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Check sam declarations.

    def check_declarations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck_declarations(self.current_stage_def.logdir, declare=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Test sam declarations.

    def test_declarations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.dotest_declarations(dim)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Create sam dataset definition.

    def define(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_definition(self.current_stage_def.defname, dim, define=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Check whether sam dataset definition exists.

    def check_definition(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_definition(self.current_stage_def.defname, dim, define=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Test sam dataset definition (list files).

    def test_definition(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        defname = self.current_stage_def.defname
        if defname == '':
            tkMessageBox.showwarning('No sam dataset definition specified.')
            return
        try:
            project.dotest_definition(defname)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Check locations.

    def check_locations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_locations(dim, self.current_stage_def.logdir,
                                      add=False,
                                      clean=False,
                                      remove=False,
                                      upload=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Add disk locations.

    def add_locations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_locations(dim, self.current_stage_def.outdir,
                                      add=True,
                                      clean=False,
                                      remove=False,
                                      upload=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Clean disk locations.

    def clean_locations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_locations(dim, self.current_stage_def.outdir,
                                      add=False,
                                      clean=True,
                                      remove=False,
                                      upload=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Remove disk locations.

    def remove_locations(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_locations(dim, self.current_stage_def.outdir,
                                      add=False,
                                      clean=False,
                                      remove=True,
                                      upload=False)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Upload.

    def upload(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            dim = project_utilities.dimensions(self.current_project_def, self.current_stage_def)
            project.docheck_locations(dim, self.current_stage_def.outdir,
                                      add=False,
                                      clean=False,
                                      remove=False,
                                      upload=True)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Do a SAM audit.

    def audit(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.doaudit(self.current_stage_def)
        except:
            e = sys.exc_info()
            traceback.print_tb(e[2])
            tkMessageBox.showerror('', e[1])

    # Help method.

    def help(self):

        # Capture output from project.py --help.
        # Because of the way this command is implemented in project.py, we have
        # to run in a separate process, not just call method help of project module.

        command = ['project.py', '--help']
        helptext = subprocess.check_output(command)
        w = TextWindow()
        w.append(helptext)

    # XML help method.

    def xmlhelp(self):

        # Capture output from project.py --xmlhelp.
        # Because of the way this command is implemented in project.py, we have
        # to run in a separate process, not just call method xmlhelp of project module.

        command = ['project.py', '--xmlhelp']
        helptext = subprocess.check_output(command)
        w = TextWindow()
        w.append(helptext)

    # Close window.

    def close(self):
        self.root.destroy()
