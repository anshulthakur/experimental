import sys
from PyQt5.QtWidgets import QWidget, QApplication, QLabel
from PyQt5.QtWidgets import QPushButton, QHBoxLayout, QVBoxLayout

from PyQt5.QtWidgets import qApp

from PyQt5.QtWidgets import QGridLayout, QLineEdit, QTextEdit

class Example(QWidget):
  def __init__(self):
    super().__init__()
    
    self.initUI()
    
  def initUI(self):
    '''
    okButton = QPushButton('OK')
    cancelButton = QPushButton('Cancel')
    
    hbox = QHBoxLayout()
    hbox.addStretch(1)
    hbox.addWidget(okButton)
    hbox.addWidget(cancelButton)
    
    vbox = QVBoxLayout()
    vbox.addStretch(1)
    vbox.addLayout(hbox)
    
    self.setLayout(vbox)
    
    #If the self is replaced by - say - okButton
    # then things are positioned inside the OK Button.
    lbl1 = QLabel('Example Usage', self)
    lbl1.move(15,10)
    
    lbl2 = QLabel('Of Absolute Postioning', self)
    lbl2.move(35, 40)
    '''
    grid = QGridLayout()
    self.setLayout(grid)
    
    names = ['Cls', 'Bck', '', 'Close',
              '7', '8', '9', '/',
              '4', '5', '6', '*',
              '1', '2', '3', '-',
              '0', '.', '=', '+']
              
    positions = [(i,j) for i in range(5) for j in range(4)]
    
    for position, name in zip(positions, names):
      if name=='':
        continue

      button = QPushButton(name)
      if name=='Close':
        button.clicked.connect(qApp.quit)      
      grid.addWidget(button, *position)
      
    authorLabel = QLabel('Author')
    authorInput = QLineEdit()
    descLabel = QLabel('Description')
    descInput = QTextEdit()
    
    grid.setSpacing(10)
    
    grid.addWidget(authorLabel, 5,0)
    grid.addWidget(authorInput, 5,1)
    grid.addWidget(descLabel, 6,0)
    grid.addWidget(descInput, 6,1)
    #self.setGeometry(300, 300, 250, 100)
    self.move(300, 150)
    self.setWindowTitle('Calculator Layout')
    self.show()
    
if __name__=='__main__':
  qa = QApplication(sys.argv)
  ex = Example()
  sys.exit(qa.exec_())
