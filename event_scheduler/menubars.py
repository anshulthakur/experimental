import sys
from PyQt5.QtWidgets import QMainWindow, QApplication, qApp, QAction, QMenu, QTextEdit
from PyQt5.QtWidgets import QDesktopWidget

from PyQt5.QtGui import QIcon

import time

class Example(QMainWindow):
  def __init__(self):
    super().__init__()
    
    self.initUI()
    
  def initUI(self):
    textEdit = QTextEdit()
    self.setCentralWidget(textEdit) #Occupy all the space that is left.
    
    #Define an Exit Menu Item in file menu
    exitAct = QAction(QIcon('exit.png'), '&Exit', self)
    exitAct.setShortcut('Ctrl+Q')
    exitAct.setStatusTip('Exit application')
    exitAct.triggered.connect(qApp.quit)

    self.statusbar = self.statusBar()
    self.statusbar.showMessage('Ready')
    
    #Create a file Menu
    menubar = self.menuBar()
    fileMenu = menubar.addMenu('&File')
    fileMenu.addAction(exitAct)
    
    #Create a submenu inside the main menu
    impMenu = QMenu('Import', self)
    impAct = QAction('Import Mail', impMenu)
    impAct.setStatusTip('Import mail.')
    impMenu.addAction(impAct)
    
    newAct = QAction('New', self)
    newAct.setStatusTip('New action')
    fileMenu.addAction(newAct)
    fileMenu.addMenu(impMenu)
    
    #Create a view menu
    viewMenu = menubar.addMenu('&View')
    viewStatusAct = QAction('View statusbar', self, checkable=True)
    viewStatusAct.setStatusTip('View statusbar')
    viewStatusAct.triggered.connect(self.toggleMenu)
    
    viewMenu.addAction(viewStatusAct)
    
    #Create a toolbar too
    exitAct = QAction(QIcon('exit.png'), 'Exit', self)
    exitAct.setShortcut('Ctrl+Q')
    exitAct.triggered.connect(qApp.quit)
    
    self.toolbar = self.addToolBar('Exit')
    self.toolbar.addAction(exitAct)
        
    self.setGeometry(300,300, 300, 200)
    self.setWindowTitle('Menu bars')
    self.show()
    
  def center(self):
    qr = self.frameGeometry()
    cp = QDesktopWidget().availableGeometry().center()
    qr.moveCenter(cp)
    self.move(qr.topLeft())
    
  def toggleMenu(self, state):
    if state:
      self.statusbar.show()
    else:
      self.statusbar.hide()

  def contextMenuEvent(self, event):
    cmenu = QMenu(self)
    newAct = cmenu.addAction("New")
    opnAct = cmenu.addAction("Open")
    quitAct = cmenu.addAction("Quit")
    action = cmenu.exec_(self.mapToGlobal(event.pos()))
    
    if action == quitAct:
      qApp.quit()
      
if __name__ == '__main__':
  app = QApplication(sys.argv)
  ex = Example()
  sys.exit(app.exec_())
