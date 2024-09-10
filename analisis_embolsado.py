from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QLabel, QRadioButton, QCheckBox, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem, QProgressBar, QFileDialog, QMessageBox, QDateEdit, QButtonGroup
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import pandas as pd
import psycopg2

class UpdateThread(QThread):
    update_progress = pyqtSignal(int)
    update_label = pyqtSignal(str)
    update_finished = pyqtSignal(pd.DataFrame, pd.DataFrame, str, str, pd.DataFrame, pd.DataFrame, str)
    update_record_id = pyqtSignal(str)  # New signal for record ID

    def run(self):
        try:
            conn_params = {
                'host': '128.254.204.227',
                'dbname': 'marbran',
                'user': 'marbran',
                'password': 'geektv2020',
                'options': '-c client_encoding=UTF8'
            }
            self.update_label.emit("Conectando a la base de datos...")
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()

            self.update_label.emit("Leyendo datos de la tabla: sistfiles_sistema_produccion")
            query_produccion = "SELECT * FROM sistfiles_sistema_produccion"
            cur.execute(query_produccion)
            data_produccion = cur.fetchall()
            columns_produccion = [desc[0] for desc in cur.description]
            df_produccion = pd.DataFrame(data_produccion, columns=columns_produccion)

            for record in data_produccion:
                self.update_record_id.emit(f"Leyendo registro: {record[0]}")  # Emit record ID

            df_produccion['clave_producto'] = df_produccion['cvepdto'].str[-2:]
            df_produccion['cvepdto'] = df_produccion['cvepdto'].str[:-2]
            df_produccion.set_index('cvepdto', inplace=True)

            self.update_label.emit("Leyendo datos de la tabla: base_calidad_organizada")
            query_calidad = "SELECT * FROM base_calidad_organizada"
            cur.execute(query_calidad)
            data_calidad = cur.fetchall()
            columns_calidad = [desc[0] for desc in cur.description]
            df_calidad = pd.DataFrame(data_calidad, columns=columns_calidad)
            df_calidad.set_index('cve_producto', inplace=True)

            for record in data_calidad:
                self.update_record_id.emit(f"Leyendo registro: {record[0]}")  # Emit record ID

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

            self.update_progress.emit(20)
            self.update_finished.emit(df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path)

        except psycopg2.Error as e:
            self.update_label.emit(f"Error de base de datos: {e}")
        except Exception as e:
            self.update_label.emit(f"Error al actualizar los datos: {e}")

class AnalisisEmbolsadoApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Actualización de Datos")
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.btn_actualizar = QPushButton("Actualizar datos")
        self.btn_actualizar.clicked.connect(self.actualizar_datos)
        self.layout.addWidget(self.btn_actualizar)

    def actualizar_datos(self):
        self.progress_window = QWidget()
        self.progress_window.setWindowTitle("Actualizando Datos")
        self.progress_layout = QVBoxLayout()
        self.progress_window.setLayout(self.progress_layout)

        self.progress_label = QLabel("Actualizando datos, por favor espere...")
        self.progress_layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_layout.addWidget(self.progress_bar)

        self.progress_window.show()

        self.update_thread = UpdateThread()
        self.update_thread.update_progress.connect(self.progress_bar.setValue)
        self.update_thread.update_label.connect(self.progress_label.setText)
        self.update_thread.update_record_id.connect(self.progress_label.setText)  # Connect new signal
        self.update_thread.update_finished.connect(self.seleccionar_dataframe)
        self.update_thread.start()

    def seleccionar_dataframe(self, df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path):
        self.progress_window.close()

        self.selection_window = QWidget()
        self.selection_window.setWindowTitle("ELIJE QUE DATOS QUIERES ANALIZAR (EMBOLSADO/IQF)")
        self.selection_layout = QVBoxLayout()
        self.selection_window.setLayout(self.selection_layout)

        self.user_choice = QButtonGroup(self)
        self.radio_embolsado = QRadioButton("EMBOLSADO")
        self.radio_iqf = QRadioButton("IQF")
        self.user_choice.addButton(self.radio_embolsado)
        self.user_choice.addButton(self.radio_iqf)
        self.selection_layout.addWidget(self.radio_embolsado)
        self.selection_layout.addWidget(self.radio_iqf)

        self.btn_select = QPushButton("Seleccionar")
        self.btn_select.clicked.connect(lambda: self.on_select(df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path))
        self.selection_layout.addWidget(self.btn_select)

        self.selection_window.show()

    def on_select(self, df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path):
        self.selection_window.close()
        if self.radio_embolsado.isChecked():
            datos_seleccionados = df_embolsado
            nombre_datos = "DATOS EMBOLSADO"
            datos_seleccionados = datos_seleccionados.merge(df_material_Cabeza, left_on='cvepdto', right_on='cve_producto', how='left')
        else:
            datos_seleccionados = df_iqf
            nombre_datos = "DATOS IQF"

        self.date_window = QWidget()
        self.date_window.setWindowTitle("Ingrese el rango de fechas")
        self.date_layout = QVBoxLayout()
        self.date_window.setLayout(self.date_layout)

        self.date_layout.addWidget(QLabel("Fecha inicial:"))
        self.fecha_inicial_entry = QDateEdit()
        self.fecha_inicial_entry.setCalendarPopup(True)
        self.date_layout.addWidget(self.fecha_inicial_entry)

        self.date_layout.addWidget(QLabel("Fecha final:"))
        self.fecha_final_entry = QDateEdit()
        self.fecha_final_entry.setCalendarPopup(True)
        self.date_layout.addWidget(self.fecha_final_entry)

        # Set the last date in the dataframe as the selected date
        if not datos_seleccionados.empty and 'fecha' in datos_seleccionados.columns:
            last_date = datos_seleccionados['fecha'].max()
            self.fecha_inicial_entry.setDate(last_date)
            self.fecha_final_entry.setDate(last_date)

        self.btn_date_select = QPushButton("Seleccionar Fechas")
        self.btn_date_select.clicked.connect(lambda: self.on_date_select(datos_seleccionados, nombre_datos, fecha_mas_antigua, fecha_mas_reciente))
        self.date_layout.addWidget(self.btn_date_select)

        self.date_window.show()

    def on_date_select(self, datos_seleccionados, nombre_datos, fecha_mas_antigua, fecha_mas_reciente):
        fecha_inicial = self.fecha_inicial_entry.date().toString("yyyy-MM-dd")
        fecha_final = self.fecha_final_entry.date().toString("yyyy-MM-dd")
        self.date_window.close()
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
        self.btn_line_select.clicked.connect(lambda: self.on_line_select(df_filtrado, nombre_datos, fecha_mas_antigua, fecha_mas_reciente))
        self.line_selection_layout.addWidget(self.btn_line_select)

        self.line_selection_window.show()

    def on_line_select(self, df_filtrado, nombre_datos, fecha_mas_antigua, fecha_mas_reciente):
        self.line_selection_window.close()
        lineas_filtradas = [linea for linea, checkbox in self.lineas_seleccionadas.items() if checkbox.isChecked()]
        datos_filtrados = df_filtrado[df_filtrado['línea'].isin(lineas_filtradas)] if lineas_filtradas else df_filtrado

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

        self.table.resizeColumnsToContents()  # Ajustar el tamaño de las columnas

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

        QMessageBox.information(self, "Éxito", f"Datos actualizados y guardados correctamente.\nFecha más antigua: {fecha_mas_antigua}\nFecha más reciente: {fecha_mas_reciente}\nDatos seleccionados: {nombre_datos}\nLíneas seleccionadas: {lineas_filtradas}")

        self.data_window.showMaximized()  # Maximizar la ventana de datos filtrados

    def exportar_excel(self, datos_filtrados):
        file_path, _ = QFileDialog.getSaveFileName(self, "Guardar archivo", "", "Excel files (*.xlsx);;All files (*)")
        if file_path:
            datos_filtrados.to_excel(file_path, index=True)
            QMessageBox.information(self, "Éxito", f"Datos exportados correctamente a {file_path}")

if __name__ == "__main__":
    app = QApplication([])
    window = AnalisisEmbolsadoApp()
    window.showMaximized()  # Maximize the window to full screen
    app.exec_()