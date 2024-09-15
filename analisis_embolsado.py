import psycopg2
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QLabel, QCheckBox, QVBoxLayout, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox, QDateEdit, QMainWindow, QAction, QLineEdit, QDialog, QFormLayout
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import pandas as pd

class UpdateThread(QThread):
    update_finished = pyqtSignal(pd.DataFrame, pd.DataFrame, str, str, pd.DataFrame, pd.DataFrame, str)

    def __init__(self, conn_params):
        super().__init__()
        self.conn_params = conn_params

    def run(self):
        try:
            conn = psycopg2.connect(**self.conn_params)
            cur = conn.cursor()

            query_produccion = "SELECT * FROM sistfiles_sistema_produccion"
            cur.execute(query_produccion)
            data_produccion = cur.fetchall()
            columns_produccion = [desc[0] for desc in cur.description]
            df_produccion = pd.DataFrame(data_produccion, columns=columns_produccion)

            df_produccion['clave_producto'] = df_produccion['cvepdto'].str[:15]

            query_calidad = "SELECT * FROM base_calidad_organizada"
            cur.execute(query_calidad)
            data_calidad = cur.fetchall()
            columns_calidad = [desc[0] for desc in cur.description]
            df_calidad = pd.DataFrame(data_calidad, columns=columns_calidad)
            df_calidad.set_index('cve_producto', inplace=True)

            output_path = 'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\base_calidad_organizada.csv'
            df_calidad.to_csv(output_path, index=True)

            df_material_Cabeza = df_calidad[['mat_cabeza']]
            output_path_material_cabeza = 'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\material_cabeza.csv'
            df_material_Cabeza.to_csv(output_path_material_cabeza, index=True)

            cur.close()
            conn.close()

            if 'fecha' in df_produccion.columns:
                df_produccion['fecha'] = pd.to_datetime(df_produccion['fecha'])
                fecha_mas_antigua = df_produccion['fecha'].min().strftime('%Y-%m-%d')
                fecha_mas_reciente = df_produccion['fecha'].max().strftime('%Y-%m-%d')
            else:
                fecha_mas_antigua = "No disponible"
                fecha_mas_reciente = "No disponible"

            df_produccion = df_produccion[df_produccion['planta_id'] != 'PLANTA 1']
            df_produccion = df_produccion.drop(columns=['id', 'estado', 'fc', 'fm', 'um', 'uc_id'])
            df_produccion['supervisor_id'] = df_produccion['concepto'].str.slice(0, 4)
            df_produccion['línea'] = df_produccion['concepto'].str.slice(4, 9)
            df_produccion['turno'] = df_produccion['concepto'].str[-1]

            df_iqf = df_produccion[df_produccion['refmovto'] == 'I.Q.F.']
            df_embolsado = df_produccion[df_produccion['refmovto'] == 'EMBOLSADO']
            df_utilizado = df_produccion[df_produccion['refmovto'] == 'UTILIZADO']

            self.update_finished.emit(df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path)

        except psycopg2.Error as e:
            print(f"Error de base de datos: {e}")
        except Exception as e:
            print(f"Error al actualizar los datos: {e}")

class ConnectionDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Actualizar Datos de Conexión")
        self.setGeometry(100, 100, 400, 200)

        self.layout = QFormLayout(self)

        self.host_input = QLineEdit(self)
        self.dbname_input = QLineEdit(self)
        self.user_input = QLineEdit(self)
        self.password_input = QLineEdit(self)
        self.password_input.setEchoMode(QLineEdit.Password)

        self.layout.addRow("Host:", self.host_input)
        self.layout.addRow("Database Name:", self.dbname_input)
        self.layout.addRow("User:", self.user_input)
        self.layout.addRow("Password:", self.password_input)

        self.btn_update = QPushButton("Actualizar", self)
        self.btn_update.clicked.connect(self.update_connection)
        self.layout.addWidget(self.btn_update)

    def update_connection(self):
        self.conn_params = {
            'host': self.host_input.text(),
            'dbname': self.dbname_input.text(),
            'user': self.user_input.text(),
            'password': self.password_input.text(),
            'options': '-c client_encoding=UTF8'
        }
        self.accept()

class AnalisisEmbolsadoApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Actualización de Datos")
        self.setGeometry(100, 100, 800, 600)

        self.statusBar().showMessage("Listo para actualizar datos")

        self.conn_params = {
            'host': '128.254.204.227',
            'dbname': 'marbran',
            'user': 'marbran',
            'password': 'geektv2020',
            'options': '-c client_encoding=UTF8'
        }

        self.update_thread = UpdateThread(self.conn_params)
        self.update_thread.update_finished.connect(self.datos_actualizados)

        self.initUI()

    def initUI(self):
        menubar = self.menuBar()
        fileMenu = menubar.addMenu('Menú')

        self.actualizarAction = QAction('Actualizar datos', self)
        self.actualizarAction.triggered.connect(self.actualizar_datos)
        fileMenu.addAction(self.actualizarAction)

        self.analizarEmbolsadoAction = QAction('Analizar Embolsado', self)
        self.analizarEmbolsadoAction.triggered.connect(lambda: self.analizar_datos(self.df_embolsado, "DATOS EMBOLSADO"))
        self.analizarEmbolsadoAction.setEnabled(False)
        fileMenu.addAction(self.analizarEmbolsadoAction)

        self.analizarIQFAction = QAction('Analizar IQF', self)
        self.analizarIQFAction.triggered.connect(lambda: self.analizar_datos(self.df_iqf, "DATOS IQF"))
        self.analizarIQFAction.setEnabled(False)
        fileMenu.addAction(self.analizarIQFAction)

        self.paretoAction = QAction('Análisis de Pareto de Productos Empacados', self)
        self.paretoAction.triggered.connect(self.analisis_pareto)
        self.paretoAction.setEnabled(False)
        fileMenu.addAction(self.paretoAction)

        self.actualizarConexionAction = QAction('Actualizar Conexión', self)
        self.actualizarConexionAction.triggered.connect(self.actualizar_conexion)
        fileMenu.addAction(self.actualizarConexionAction)

        self.salirAction = QAction('Salir del sistema', self)
        self.salirAction.triggered.connect(QApplication.quit)
        fileMenu.addAction(self.salirAction)

    def actualizar_datos(self):
        self.statusBar().showMessage("Actualizando datos...")
        self.update_thread.start()

    def actualizar_conexion(self):
        dialog = ConnectionDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            self.conn_params = dialog.conn_params
            self.update_thread.conn_params = self.conn_params
            QMessageBox.information(self, "Éxito", "Datos de conexión actualizados correctamente.")

    def datos_actualizados(self, df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path):
        self.df_embolsado = df_embolsado
        self.df_iqf = df_iqf
        self.fecha_mas_antigua = fecha_mas_antigua
        self.fecha_mas_reciente = fecha_mas_reciente
        self.df_calidad = df_calidad
        self.df_material_Cabeza = df_material_Cabeza
        self.output_path = output_path

        self.statusBar().showMessage("Datos actualizados correctamente")

        # Habilitar las demás opciones del menú
        self.analizarEmbolsadoAction.setEnabled(True)
        self.analizarIQFAction.setEnabled(True)
        self.paretoAction.setEnabled(True)

    def analizar_datos(self, datos_seleccionados, nombre_datos):
        self.selection_window = QWidget()
        self.selection_window.setWindowTitle(f"Análisis de {nombre_datos}")
        self.selection_layout = QVBoxLayout()
        self.selection_window.setLayout(self.selection_layout)

        self.date_layout = QVBoxLayout()
        self.selection_layout.addLayout(self.date_layout)

        self.date_layout.addWidget(QLabel("Fecha inicial:"))
        self.fecha_inicial_entry = QDateEdit()
        self.fecha_inicial_entry.setCalendarPopup(True)
        self.date_layout.addWidget(self.fecha_inicial_entry)

        self.date_layout.addWidget(QLabel("Fecha final:"))
        self.fecha_final_entry = QDateEdit()
        self.fecha_final_entry.setCalendarPopup(True)
        self.date_layout.addWidget(self.fecha_final_entry)

        if not datos_seleccionados.empty and 'fecha' in datos_seleccionados.columns:
            last_date = datos_seleccionados['fecha'].max()
            self.fecha_inicial_entry.setDate(last_date)
            self.fecha_final_entry.setDate(last_date)

        self.btn_date_select = QPushButton("Seleccionar Fechas")
        self.btn_date_select.clicked.connect(lambda: self.on_date_select(datos_seleccionados, nombre_datos))
        self.date_layout.addWidget(self.btn_date_select)

        self.selection_window.show()

    def on_date_select(self, datos_seleccionados, nombre_datos):
        fecha_inicial = self.fecha_inicial_entry.date().toString("yyyy-MM-dd")
        fecha_final = self.fecha_final_entry.date().toString("yyyy-MM-dd")
        self.selection_window.close()
        df_filtrado = datos_seleccionados[(datos_seleccionados['fecha'] >= fecha_inicial) & (datos_seleccionados['fecha'] <= fecha_final)]

        self.line_selection_window = QWidget()
        self.line_selection_window.setWindowTitle("Selecciona las líneas que deseas analizar")
        self.line_selection_layout = QVBoxLayout()
        self.line_selection_window.setLayout(self.line_selection_layout)

        lineas_unicas = df_filtrado['línea'].unique()
        self.lineas_seleccionadas = {linea: QCheckBox(linea) for linea in lineas_unicas}

        for linea, checkbox in self.lineas_seleccionadas.items():
            self.line_selection_layout.addWidget(checkbox)

        self.btn_line_select = QPushButton("Seleccionar Líneas")
        self.btn_line_select.clicked.connect(lambda: self.on_line_select(df_filtrado, nombre_datos))
        self.line_selection_layout.addWidget(self.btn_line_select)

        self.line_selection_window.show()

    def on_line_select(self, df_filtrado, nombre_datos):
        self.line_selection_window.close()
        lineas_filtradas = [linea for linea, checkbox in self.lineas_seleccionadas.items() if checkbox.isChecked()]
        datos_filtrados = df_filtrado[df_filtrado['línea'].isin(lineas_filtradas)] if lineas_filtradas else df_filtrado

        if nombre_datos == "Análisis de Pareto":
            self.mostrar_pareto(datos_filtrados, lineas_filtradas)
        else:
            self.mostrar_datos_filtrados(datos_filtrados, nombre_datos, lineas_filtradas)

    def mostrar_datos_filtrados(self, datos_filtrados, nombre_datos, lineas_filtradas):
        self.data_window = QWidget()
        self.data_window.setWindowTitle("Datos Filtrados")
        self.data_layout = QVBoxLayout()
        self.data_window.setLayout(self.data_layout)

        self.table = QTableWidget()
        self.table.setRowCount(len(datos_filtrados))
        self.table.setColumnCount(len(datos_filtrados.columns))
        self.table.setHorizontalHeaderLabels(datos_filtrados.columns)

        for i, row in enumerate(datos_filtrados.itertuples()):
            for j, value in enumerate(row[1:]):
                self.table.setItem(i, j, QTableWidgetItem(str(value)))

        self.table.resizeColumnsToContents()

        self.data_layout.addWidget(self.table)

        self.btn_export = QPushButton("Exportar a Excel")
        self.btn_export.clicked.connect(lambda: self.exportar_excel(datos_filtrados))
        self.data_layout.addWidget(self.btn_export)

        self.btn_close = QPushButton("Cerrar")
        self.btn_close.clicked.connect(self.data_window.close)
        self.data_layout.addWidget(self.btn_close)

        self.btn_exit = QPushButton("Salir del Sistema")
        self.btn_exit.clicked.connect(QApplication.quit)
        self.data_layout.addWidget(self.btn_exit)

        output_path_filtrado = f'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\resultados_{nombre_datos.lower().replace(" ", "_")}.xlsx'
        datos_filtrados.to_excel(output_path_filtrado, index=True)

        QMessageBox.information(self, "Éxito", f"Datos actualizados y guardados correctamente.\nFecha más antigua: {self.fecha_mas_antigua}\nFecha más reciente: {self.fecha_mas_reciente}\nDatos seleccionados: {nombre_datos}\nLíneas seleccionadas: {lineas_filtradas}")

        self.data_window.showMaximized()

    def exportar_excel(self, datos_filtrados):
        file_path, _ = QFileDialog.getSaveFileName(self, "Guardar archivo", "", "Excel files (*.xlsx);;All files (*)")
        if file_path:
            datos_filtrados.to_excel(file_path, index=True)
            QMessageBox.information(self, "Éxito", f"Datos exportados correctamente a {file_path}")

    def analisis_pareto(self):
        self.analizar_datos(self.df_embolsado, "Análisis de Pareto")

    def exportar_pareto_excel(self, df_pareto):
        file_path, _ = QFileDialog.getSaveFileName(self, "Guardar archivo", "", "Excel files (*.xlsx);;All files (*)")
        if file_path:
            df_pareto.to_excel(file_path, index=True)
            QMessageBox.information(self, "Éxito", f"Datos exportados correctamente a {file_path}")

    def mostrar_pareto(self, datos_filtrados, lineas_filtradas):
        # Agrupar por clave_producto y línea, y calcular la sumatoria de libras
        datos_agrupados = datos_filtrados.groupby(['clave_producto', 'línea']).agg({
            'descpdto': 'first',
            'libras': 'sum'
        }).reset_index()

        # Calcular el total de libras por línea
        total_libras_por_linea = datos_agrupados.groupby('línea')['libras'].sum()

        # Calcular el porcentaje de contribución de cada producto
        datos_agrupados['porcentaje_contribucion'] = datos_agrupados.apply(
            lambda row: (row['libras'] / total_libras_por_linea[row['línea']]) * 100, axis=1
        )

        # Ordenar por línea y luego por porcentaje_contribucion de mayor a menor
        datos_agrupados = datos_agrupados.sort_values(by=['línea', 'porcentaje_contribucion'], ascending=[True, False])

        # Crear una lista para almacenar las filas de la tabla
        table_data = []

        # Agregar filas de datos y totales por línea
        for linea, group in datos_agrupados.groupby('línea'):
            group = group.copy()
            group['porcentaje_acumulado'] = group['porcentaje_contribucion'].cumsum()
            table_data.extend(group[['clave_producto', 'descpdto', 'libras', 'línea', 'porcentaje_contribucion', 'porcentaje_acumulado']].values.tolist())
            total_libras = total_libras_por_linea[linea]
            total_porcentaje = group['porcentaje_contribucion'].sum()
            table_data.append(['Total', '', total_libras, linea, total_porcentaje, ''])

        df_pareto = pd.DataFrame(table_data, columns=['clave_producto', 'descpdto', 'libras', 'línea', 'porcentaje_contribucion', 'porcentaje_acumulado'])

        self.pareto_window = QWidget()
        self.pareto_window.setWindowTitle("Análisis de Pareto de Productos Empacados")
        self.pareto_layout = QVBoxLayout()
        self.pareto_window.setLayout(self.pareto_layout)

        self.pareto_table = QTableWidget()
        self.pareto_table.setRowCount(len(df_pareto))
        self.pareto_table.setColumnCount(len(df_pareto.columns))
        self.pareto_table.setHorizontalHeaderLabels(df_pareto.columns)

        for i, row in enumerate(df_pareto.itertuples()):
            for j, value in enumerate(row[1:]):
                if df_pareto.columns[j] == 'porcentaje_contribucion' and value != '':
                    value = f"{value:.1f}"  # Formatear con 1 cifra significativa
                item = QTableWidgetItem(str(value))
                if df_pareto.columns[j] == 'porcentaje_acumulado' and value != '' and float(value) <= 80:
                    item.setBackground(Qt.yellow)
                self.pareto_table.setItem(i, j, item)

        self.pareto_table.resizeColumnsToContents()
        self.pareto_layout.addWidget(self.pareto_table)

        self.btn_export_pareto = QPushButton("Exportar a Excel")
        self.btn_export_pareto.clicked.connect(lambda: self.exportar_pareto_excel(df_pareto))
        self.pareto_layout.addWidget(self.btn_export_pareto)

        self.btn_back = QPushButton("Regresar al menú principal")
        self.btn_back.clicked.connect(self.pareto_window.close)
        self.pareto_layout.addWidget(self.btn_back)

        self.pareto_window.show()

if __name__ == "__main__":
    app = QApplication([])
    window = AnalisisEmbolsadoApp()
    window.showMaximized()
    app.exec_()