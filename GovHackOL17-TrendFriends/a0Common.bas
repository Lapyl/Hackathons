Attribute VB_Name = "a0Common"
Public Sub RngFun(sRange As Range, sFormula As String)
    With sRange
        .FormulaR1C1 = "=" & sFormula
        .Copy
        .PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
    End With
    Application.CutCopyMode = False
End Sub

Public Sub fMapNam(sState As String)
    On Error Resume Next
    With Selection.ShapeRange.TextFrame2
        .TextRange.ParagraphFormat.Alignment = msoAlignCenter
        .VerticalAnchor = msoAnchorMiddle
        .HorizontalAnchor = msoAnchorNone
        .MarginLeft = 0
        .MarginTop = 0
        .MarginRight = 0
        .MarginBottom = 0
        .WordWrap = msoFalse
    End With
    Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = sState
    Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, 5).ParagraphFormat.FirstLineIndent = 0
    Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, 5).ParagraphFormat.Alignment = msoAlignCenter
    With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, 5).Font
        .NameComplexScript = "+mn-cs"
        .NameFarEast = "+mn-ea"
        .Fill.Visible = msoTrue
        .Fill.ForeColor.ObjectThemeColor = msoThemeColorLight1
        .Fill.ForeColor.TintAndShade = 0
        .Fill.ForeColor.Brightness = 0
        .Fill.Transparency = 0
        .Fill.Solid
        .Size = 11
        .Name = "+mn-lt"
    End With
    On Error GoTo 0
End Sub

Public Sub fFormat(iI As Integer)
    Cells.HorizontalAlignment = xlLeft
    Cells.VerticalAlignment = xlTop
    Cells.MergeCells = False
    Cells.Font.Name = "Times New Roman"
    Cells.Font.Size = 12
    With Cells.Interior
        .Pattern = xlSolid
        .PatternColorIndex = xlAutomatic
        .ThemeColor = xlThemeColorAccent6
        .TintAndShade = 0.8
        .PatternTintAndShade = 0
    End With
    Columns.AutoFit
    Range("A1").Select
    'ActiveSheet.Protect DrawingObjects:=True, Contents:=True, Scenarios:=True
End Sub

Public Sub fTransfer(sFrom As String, sTo As String, sModule As String)
  Workbooks(sFrom).Activate
  ActiveWorkbook.VBProject.VBComponents(sModule).Export ("C:\temp\" & sModule & ".bas")
  Workbooks(sTo).Activate
  ActiveWorkbook.VBProject.VBComponents.Import ("C:\temp\" & sModule & ".bas")
End Sub

Public Sub fGetFile(k As Integer)

    Dim oNam As Variant
    
    oNam = Application.GetOpenFilename(FileFilter:="All files (*.*), *.*", Title:="Select file to be processed.")
    
    If oNam = False Then
        MsgBox "No file selected", vbOKOnly, "Existing"
        Exit Sub
    End If
    
    'If (Right(sNam, 4) = ".csv" Or Right(sNam, 4) = ".xls" Or Right(sNam, 5) = ".xlsx" Or Right(sNam, 5) = ".xlsm") Then
        Workbooks.Open Filename:=oNam
    'Else
        'MsgBox "Spreadsheet file not selected", vbOKOnly, "Exiting"
        'Exit Sub
    'End If
    
End Sub

Public Function fGetDir(sXyz As String) As String

    Dim oDlg As FileDialog
    Dim sItm As String
    
    Set oDlg = Application.FileDialog(msoFileDialogFolderPicker)
    
    With oDlg
        .Title = sXyz
        .AllowMultiSelect = False
        .InitialFileName = "C:\"
        If .Show <> -1 Then GoTo zEnd
        sItm = .SelectedItems(1)
    End With
    
zEnd:
    fGetDir = sItm
    Set oDlg = Nothing
    
End Function

Public Function fSetFun(iArg As Double, iTyp As Integer) As Double
    fSetFun = 0
    On Error GoTo xExit
    Select Case iTyp
        Case 3
            fSetFun = (iArg) ^ 1
        Case 4
            fSetFun = (iArg) ^ 2
        Case 5
            fSetFun = (iArg) ^ 3
        Case 6
            fSetFun = (iArg) ^ 4
        Case 7
            fSetFun = (iArg) ^ (-1)
        Case 8
            fSetFun = (iArg) ^ (-2)
        Case 9
            fSetFun = (iArg) ^ (-3)
        Case 10
            fSetFun = (iArg) ^ (-4)
        Case 11
            fSetFun = Exp((iArg) ^ 1)
        Case 12
            fSetFun = Exp((iArg) ^ 2)
        Case 13
            fSetFun = Exp((iArg) ^ (-1))
        Case 14
            fSetFun = Exp((iArg) ^ (-2))
        Case 15
            fSetFun = Sin((iArg) ^ 1)
        Case 16
            fSetFun = Sin((iArg) ^ 2)
        Case 17
            fSetFun = Sin((iArg) ^ (-1))
        Case 18
            fSetFun = Sin((iArg) ^ (-2))
        Case 19
            fSetFun = Log((iArg))
    End Select
xExit:
End Function
