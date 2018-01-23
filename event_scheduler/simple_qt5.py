import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QToolTip, QPushButton, QMessageBox, QDesktopWidget)
from PyQt5.QtGui import (QIcon, QFont)
from PyQt5.QtCore import QCoreApplication

class Example(QWidget):
  def __init__(self):
    super().__init__()
    self.initUI()
    
  def initUI(self):
    #Set Tooltip and its options
    QToolTip.setFont(QFont('SansSerif', 10))
    self.setToolTip('This is a <b>QWidget</b> widget')
    
    #place button
    btn = QPushButton('Quit', parent = self)
    btn.setToolTip('This is a <b>QPushButton</b> widget')
    btn.resize(btn.sizeHint()) #Try replacing this with other params
    btn.move(10,10) #Within relative positioning of widget, place button at (x,y)
    
    #On clicking button, signal `clicked` is emitted.
    btn.clicked.connect(QCoreApplication.instance().quit)
    
    #Set Window attributes and show
    self.setGeometry(100,100,300,220) #x-origin, y-origin, x-len, y-len
    self.setWindowTitle('Icon')
    self.setWindowIcon(QIcon('timer.png'))
    
    self.center()
    self.show()
  
  def center(self):
    qr = self.frameGeometry()
    cp = QDesktopWidget().availableGeometry().center()
    qr.moveCenter(cp)
    self.move(qr.topLeft())

  def closeEvent(self, event):
    #On closing a QWidget, a QCloseEvent is generated.
    # So, we override its closeEvent() handler.
    reply = QMessageBox.question(self, 'Message', 
               "Are you sure you want to quit?", 
               QMessageBox.Yes|QMessageBox.No, 
               QMessageBox.No)
    if reply == QMessageBox.Yes:
      event.accept()
    else:
      event.ignore()
      
if __name__=='__main__':
  app = QApplication(sys.argv)
  w = Example()
  #w.resize(250,150)
  #w.move(100, 100)
  #w.setWindowTitle('Simple')
  #w.show()

  sys.exit(app.exec_())
