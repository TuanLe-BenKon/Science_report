# -*- coding: utf-8 -*-

import os
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.colors import Color
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, PageBreak, Image, Spacer, Table, TableStyle, Flowable)
from reportlab.lib.enums import TA_CENTER
from reportlab.graphics.shapes import  Image as sImage
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


def getWeekdayName(time: datetime) -> str:
    names = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
    return names[time.weekday()]


class ReportConfig:
    def __init__(self) -> None:
        self.marginTop = 2.5*cm
        self.marginBottom = 2.5*cm
        self.marginLeft = 2.0*cm
        self.marginRight = 2.0*cm
        self.pageWidth, self.pageHeight = A4
        self.bodyWidth = self.pageWidth - self.marginLeft - self.marginRight
        self.bodyHeight = self.pageHeight - self.marginTop - self.marginBottom

    def registerFont(self):
        pdfmetrics.registerFont(
            TTFont('NotoSans', './fonts/NotoSans-Regular.ttf'))
        pdfmetrics.registerFont(
            TTFont('NotoSansB', './fonts/NotoSans-Bold.ttf'))
        pdfmetrics.registerFont(
            TTFont('NotoSansBI', './fonts/NotoSans-BoldItalic.ttf'))
        pdfmetrics.registerFont(
            TTFont('NotoSansI', './fonts/NotoSans-Italic.ttf'))
        pdfmetrics.registerFontFamily('NotoSans', normal='NotoSans',
                                      bold='NotoSansB', italic='NotoSansI', boldItalic='NotoSansBI')


class FooterCanvas(canvas.Canvas):
    def __init__(self, filename, config: ReportConfig = ReportConfig(), *args, **kwargs):
        canvas.Canvas.__init__(self, filename, *args, **kwargs)
        self.pages = []
        self.conf = config
        self.conf.registerFont()

    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            self.draw_canvas(page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_canvas(self, page_count):
        page = "Trang %s/%s" % (self._pageNumber, page_count)
        now = datetime.now()
        dateTime = getWeekdayName(
            now) + now.strftime(", %d/%m/%Y %I:%M %p")
        self.saveState()
        self.setStrokeColorRGB(0, 0, 0)
        self.setLineWidth(1)
        self.drawImage("icons/logo.png", self.conf.marginLeft, self.conf.pageHeight - self.conf.marginBottom,
                       width=150, height=50, preserveAspectRatio=True, mask='auto')
        self.line(self.conf.marginLeft, self.conf.pageHeight-self.conf.marginBottom,
                  self.conf.pageWidth - self.conf.marginRight, self.conf.pageHeight-self.conf.marginBottom)
        self.line(self.conf.marginLeft, self.conf.marginTop,
                  self.conf.pageWidth - self.conf.marginRight, self.conf.marginTop)
        self.setFont('NotoSans', 10)
        self.drawString(self.conf.marginLeft,
                        self.conf.marginBottom-0.5*cm, dateTime)
        self.drawRightString(self.conf.pageWidth - self.conf.marginLeft,
                             self.conf.marginBottom-0.5*cm, page)
        self.restoreState()


class Energy(Flowable):
    def __init__(self, energy_kwh: float, rect_size: float = 100.0):
        super().__init__()
        self.energy_kwh = energy_kwh
        self.width = rect_size
        self.height = rect_size
        self.hAlign = "CENTER"

    def draw(self):
        self.canv.saveState()
        self.canv.setLineWidth(2)
        self.canv.setStrokeColor(Color(50.0/255, 115.0/255, 50.0/255, 1))
        self.canv.roundRect(0, 0, self.width, self.height, self.width/6)
        self.canv.drawImage("icons/energy.png", 35, 60,
                            width=30, height=30, preserveAspectRatio=True, mask='auto')
        self.canv.restoreState()
        self.canv.setFont('NotoSansB', 16)
        self.canv.setFillColor(Color(50.0/255, 115.0/255, 50.0/255, 1))
        self.canv.drawCentredString(50, 25, "%.3f kWh" % self.energy_kwh)


class ACActivity:
    def __init__(self, type: str, power_status: str, op_mode: str, op_time: str, configured_temp: str, fan_speed: str) -> None:
        self.type = type
        self.power_status = power_status
        self.op_mode = op_mode
        self.op_time = op_time
        self.configured_temp = configured_temp
        self.fan_speed = fan_speed


class BenKonReportData:
    def __init__(self, user: str, device: str, report_date: datetime, energy_kwh: float, chart_url: str, activities: "list[ACActivity]") -> None:
        self.user = user
        self.device = device
        self.report_date = report_date
        self.energy_kwh = energy_kwh
        self.chart_url = chart_url
        self.activities = activities


class BenKonReport:
    def __init__(
            self,
            path: str,
            isGenSummaryPage: bool,
            url_pie_chart: str,
            url_bar_chart: str,
            data: "list[BenKonReportData]",
            config: ReportConfig = ReportConfig()
    ):
        # Create path if not exists
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        # Initialization
        self.conf = config
        self.conf.registerFont()
        self.path = path
        self.data = data
        self.url_pie_chart = url_pie_chart
        self.url_bar_chart = url_bar_chart
        self.styleSheet = getSampleStyleSheet()
        self.elements = []

        self.colorBlue = Color((54.0/255), (122.0/255), (179.0/255), 1)
        self.colorWhite = Color(1, 1, 1, 1)
        self.colorGreen = Color(50.0/255, 115.0/255, 50.0/255, 1)
        self.colorGrey = Color(192.0/255, 192.0/255, 192.0/255, 1)
        self.colorBKLight = Color((246.0/255), (246.0/255), (247.0/255), 1)
        self.colorBKLightGray = Color((84.0/255), (181.0/255), (236.0/255), 1)
        self.colorBKNormal = Color((50.0/255), (123.0/255), (198.0/255), 1)
        self.colorBKDarkGray = Color((183.0/255), (143.0/255), (109.0/255), 1)
        self.colorBKDark = Color((31.0/255), (74.0/255), (154.0/255), 1)

        # Create page content
        if isGenSummaryPage:
            self.summaryPage()
        for idx in range(len(self.data)):
            self.firstPage(idx)
            self.activityPage(idx)

        # Build
        self.doc = SimpleDocTemplate(
            path, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm,
            topMargin=1.5*cm, bottomMargin=1.5*cm)
        self.doc.multiBuild(self.elements, canvasmaker=FooterCanvas)

    def summaryPage(self):
        # Pie chart title
        self.elements.append(Spacer(10, 1 * cm))
        chartTitleText = "Tổng điện năng tiêu thụ và thời gian hoạt động (3 ngày gần nhất)"
        chartTitleStyle = ParagraphStyle(
            name="chartTitleStyle", fontName="NotoSans", fontSize=14, alignment=TA_CENTER)
        chartTitle = Paragraph("<b>%s</b>" % chartTitleText, chartTitleStyle)
        self.elements.append(chartTitle)

        # Pie chart image
        self.elements.append(Spacer(10, 0.25 * cm))
        if self.url_pie_chart == '':
            pass
        else:
            imgChart = Image(self.url_pie_chart)
            chartWidth = self.conf.pageWidth
            chartHeight = self.conf.pageHeight
            imgChart.drawHeight = chartHeight * 0.25
            imgChart.drawWidth = chartWidth * 0.6
            imgChart.hAlign = 'CENTER'
            self.elements.append(imgChart)

        # Bar chart image
        if self.url_bar_chart == '':
            pass
        else:
            imgChart = Image(self.url_bar_chart)
            chartWidth = self.conf.pageWidth
            chartHeight = self.conf.pageHeight
            imgChart.drawHeight = chartHeight * 0.55
            imgChart.drawWidth = chartWidth
            imgChart.hAlign = 'CENTER'
            self.elements.append(imgChart)

        self.elements.append(PageBreak())

    def firstPage(self, dataIndex: int):

        # report title
        self.elements.append(Spacer(10, 1.25 * cm))
        reportTitleStyle = ParagraphStyle(
            name="reportTitleStyle", fontName="NotoSans", fontSize=20, alignment=TA_CENTER)
        title = "BÁO CÁO HOẠT ĐỘNG MÁY LẠNH"
        reportTitle = Paragraph("<b>%s</b>" % title, reportTitleStyle)
        self.elements.append(reportTitle)

        # report date
        self.elements.append(Spacer(10, 0.5 * cm))
        dateTime = getWeekdayName(
            self.data[dataIndex].report_date) + self.data[dataIndex].report_date.strftime(", %d/%m/%Y")

        reportDateStyle = ParagraphStyle(
            name="reportDateStyle", fontName="NotoSans", fontSize=12, alignment=TA_CENTER)
        reportDate = Paragraph("<b>%s</b>" % dateTime, reportDateStyle)
        self.elements.append(reportDate)

        # Header info
        iconSize = 0.7*cm
        spacer = Spacer(10, 0.5*cm)
        self.elements.append(spacer)

        imgUser = Image('icons/username.png')
        imgUser.drawHeight = iconSize
        imgUser.drawWidth = iconSize
        imgUser.hAlign = 'LEFT'

        imgAC = Image('icons/ac.png')
        imgAC.drawHeight = iconSize
        imgAC.drawWidth = iconSize
        imgAC.hAlign = 'LEFT'

        labelStyle = ParagraphStyle(
            name="Label", fontName="NotoSans")
        valueStyle = ParagraphStyle(
            name="Value", borderWidth=3, fontName="NotoSans")

        rowUser = [imgUser, Paragraph("Khách hàng:", labelStyle), Paragraph(
            "<b>%s</b>" % self.data[dataIndex].user, valueStyle)]
        rowAC = [imgAC, Paragraph("Tên thiết bị:", labelStyle), Paragraph(
            "<b>%s</b>" % self.data[dataIndex].device, valueStyle)]

        tableData = [rowUser, rowAC]
        colWidths = [iconSize+0.3*cm, 2.5*cm, 1*cm]
        colWidths[2] = self.conf.bodyWidth - colWidths[0] - colWidths[1]
        titleTable = Table(tableData, colWidths=colWidths)
        titleTableStyle = TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'CENTER'),
        ])
        titleTable.setStyle(titleTableStyle)
        self.elements.append(titleTable)

        # chart title
        self.elements.append(Spacer(10, 1 * cm))
        chartTitleText = "Biểu đồ trạng thái máy lạnh trong ngày"
        chartTitleStyle = ParagraphStyle(
            name="chartTitleStyle", fontName="NotoSans", fontSize=16, alignment=TA_CENTER)
        chartTitle = Paragraph("<b>%s</b>" % chartTitleText, chartTitleStyle)
        self.elements.append(chartTitle)

        # chart image

        if self.data[dataIndex].chart_url == '':
            pass
        else:
            self.elements.append(Spacer(10, 0.25 * cm))
            imgChart = Image(self.data[dataIndex].chart_url)
            chartSize = self.conf.pageWidth
            imgChart.drawHeight = chartSize * 0.8
            imgChart.drawWidth = chartSize
            imgChart.hAlign = 'CENTER'
            self.elements.append(imgChart)

        # page break
        self.elements.append(PageBreak())

    def _getTableColumnWidth(self, percentageWidth: "list[float]") -> "list[float]":
        tableMaxWidth = self.conf.pageWidth - \
            self.conf.marginLeft - self.conf.marginRight
        totalWidth = 0
        for colWidth in percentageWidth:
            totalWidth += colWidth
        result = []
        for colWidth in percentageWidth:
            result.append(colWidth*tableMaxWidth/totalWidth)
        return result

    def activityPage(self, dataIndex: int):

        # Energy title
        self.elements.append(Spacer(10, 1.5*cm))
        chartTitleText = "Tổng điện năng tiêu thụ"
        chartTitleStyle = ParagraphStyle(
            name="chartTitleStyle", fontName="NotoSans", fontSize=16, alignment=TA_CENTER)
        chartTitle = Paragraph("<b>%s</b>" % chartTitleText, chartTitleStyle)
        self.elements.append(chartTitle)

        # Energy rect
        self.elements.append(Spacer(10, 1*cm))
        self.elements.append(
            Energy(energy_kwh=self.data[dataIndex].energy_kwh))

        # Activities table
        spacer = Spacer(10, 1.5*cm)
        self.elements.append(spacer)
        psHeaderText = ParagraphStyle(
            'Hed0', fontSize=14, alignment=TA_CENTER, borderWidth=3, textColor=self.colorBlue, fontName="NotoSans")
        paragraphReportHeader = Paragraph(
            'Bảng các hoạt động trong ngày của máy lạnh', psHeaderText)
        self.elements.append(paragraphReportHeader)

        spacer = Spacer(10, 22)
        self.elements.append(spacer)

        """
        Create the line items
        """
        d = []
        textData = ["STT", "Thời gian", "Hoạt động", "Trạng thái", "Chế độ",
                    "Nhiệt độ thiết lập", "Tốc độ quạt"]

        fontSize = 8
        centered = ParagraphStyle(
            name="centered", alignment=TA_CENTER, fontName="NotoSans")
        for text in textData:
            ptext = "<font size='%s'><b>%s</b></font>" % (fontSize, text)
            titlesTable = Paragraph(ptext, centered)
            d.append(titlesTable)

        data = [d]
        formattedLineData = []

        alignStyle = [ParagraphStyle(name="01", alignment=TA_CENTER),
                      ParagraphStyle(name="02", alignment=TA_CENTER),
                      ParagraphStyle(name="03", alignment=TA_CENTER),
                      ParagraphStyle(name="04", alignment=TA_CENTER),
                      ParagraphStyle(name="05", alignment=TA_CENTER),
                      ParagraphStyle(name="06", alignment=TA_CENTER),
                      ParagraphStyle(name="07", alignment=TA_CENTER)]

        for id, activity in enumerate(self.data[dataIndex].activities):
            lineData = [str(id+1), activity.op_time, activity.type, activity.power_status,
                        activity.op_mode, activity.configured_temp, activity.fan_speed]
            columnNumber = 0
            for item in lineData:
                ptext = "<font size='%s'>%s</font>" % (fontSize-1, item)
                p = Paragraph(ptext, alignStyle[columnNumber])
                formattedLineData.append(p)
                columnNumber = columnNumber + 1
            data.append(formattedLineData)
            formattedLineData = []

        ''' Set table Columns width '''
        table = Table(data, colWidths=self._getTableColumnWidth(
            [7, 15, 18, 15, 15, 16, 15]))
        tStyle = TableStyle([  # ('GRID',(0, 0), (-1, -1), 0.5, grey),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'CENTER'),
            ("ALIGN", (1, 0), (1, -1), 'RIGHT'),
            ('LINEBELOW', (0, 0), (-1, -1), 0.5, self.colorGrey),
            ('BACKGROUND', (0, 0), (-1, 0), self.colorBKLightGray),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [self.colorBKLight, self.colorWhite]),
            # ('SPAN', (0, -1), (-2, -1))
        ])
        table.setStyle(tStyle)
        self.elements.append(table)

        # page break
        self.elements.append(PageBreak())
