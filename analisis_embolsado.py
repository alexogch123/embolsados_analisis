import tkinter as tk
from tkinter import messagebox, ttk, filedialog
from tkcalendar import DateEntry
import psycopg2
import pandas as pd
import threading

class AnalisisEmbolsadoApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Actualización de Datos")

        # Crear el botón "Actualizar datos"
        self.btn_actualizar = tk.Button(self.root, text="Actualizar datos", command=self.actualizar_datos)
        self.btn_actualizar.pack(pady=20)

    def actualizar_datos(self):
        def run_update():
            try:
                # Parámetros de conexión
                conn_params = {
                    'host': '128.254.204.227',
                    'database': 'marbran',
                    'user': 'marbran',
                    'password': 'geektv2020',
                    'options': '-c client_encoding=UTF8'
                }

                # Conectar a la base de datos
                conn = psycopg2.connect(**conn_params)

                # Crear un cursor
                cur = conn.cursor()

                # Leer datos de la tabla sistfiles_sistema_produccion
                progress_label.config(text="Leyendo datos de la tabla: sistfiles_sistema_produccion")
                query_produccion = "SELECT * FROM sistfiles_sistema_produccion"
                cur.execute(query_produccion)

                # Cargar los datos en un DataFrame de pandas
                data_produccion = cur.fetchall()
                columns_produccion = [desc[0] for desc in cur.description]
                df_produccion = pd.DataFrame(data_produccion, columns=columns_produccion)

                # Modificar la columna 'cvepdto' y crear la columna 'clave_producto'
                df_produccion['clave_producto'] = df_produccion['cvepdto'].str[-2:]
                df_produccion['cvepdto'] = df_produccion['cvepdto'].str[:-2]

                # Definir 'cvepdto' como la llave principal
                df_produccion.set_index('cvepdto', inplace=True)

                # Leer datos de la tabla base_calidad_organizada
                progress_label.config(text="Leyendo datos de la tabla: base_calidad_organizada")
                query_calidad = "SELECT * FROM base_calidad_organizada"
                cur.execute(query_calidad)

                # Cargar los datos en un DataFrame de pandas
                data_calidad = cur.fetchall()
                columns_calidad = [desc[0] for desc in cur.description]
                df_calidad = pd.DataFrame(data_calidad, columns=columns_calidad)

                # Definir 'cve_producto' como la llave principal
                df_calidad.set_index('cve_producto', inplace=True)

                # Definir la ruta de salida para el archivo CSV
                output_path = 'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\base_calidad_organizada.csv'

                # Exportar df_calidad a un archivo CSV
                df_calidad.to_csv(output_path, index=True)

                # Crear el DataFrame df_material_Cabeza usando las columnas 'cve_producto' y 'mat_cabeza'
                df_material_Cabeza = df_calidad[['mat_cabeza']]

                # Definir la ruta de salida para el archivo CSV de df_material_Cabeza
                output_path_material_cabeza = 'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\material_cabeza.csv'

                # Exportar df_material_Cabeza a un archivo CSV
                df_material_Cabeza.to_csv(output_path_material_cabeza, index=True)

                # Cerrar el cursor y la conexión
                cur.close()
                conn.close()

                # Asegurarse de que la columna de fecha está presente y convertirla al tipo datetime
                if 'fecha' in df_produccion.columns:
                    df_produccion['fecha'] = pd.to_datetime(df_produccion['fecha'])
                    fecha_mas_antigua = df_produccion['fecha'].min()
                    fecha_mas_reciente = df_produccion['fecha'].max()
                else:
                    fecha_mas_antigua = "No disponible"
                    fecha_mas_reciente = "No disponible"

                # Eliminar las filas donde la columna 'planta_id' sea igual a 'PLANTA 1'
                df_produccion = df_produccion[df_produccion['planta_id'] != 'PLANTA 1']

                # Eliminar las columnas 'id', 'estado', 'fc', 'fm', 'um', 'uc_id'
                df_produccion = df_produccion.drop(columns=['id', 'estado', 'fc', 'fm', 'um', 'uc_id'])

                # Extraer los primeros 4 caracteres de la columna 'concepto' y crear una nueva columna 'supervisor_id'
                df_produccion['supervisor_id'] = df_produccion['concepto'].str.slice(0, 4)

                # Extraer los caracteres del 5 al 9 de la columna 'concepto' y crear una nueva columna 'línea'
                df_produccion['línea'] = df_produccion['concepto'].str.slice(4, 9)

                # Extraer el último carácter de la columna 'concepto' y crear una nueva columna 'turno'
                df_produccion['turno'] = df_produccion['concepto'].str[-1]

                # Dividir el DataFrame en tres según el valor de la columna 'refmovto'
                df_iqf = df_produccion[df_produccion['refmovto'] == 'I.Q.F.']
                df_embolsado = df_produccion[df_produccion['refmovto'] == 'EMBOLSADO']
                df_utilizado = df_produccion[df_produccion['refmovto'] == 'UTILIZADO']

                # Actualizar la barra de progreso
                progress_var.set(20)

                # Desactivar el botón de "Actualizar datos"
                self.btn_actualizar.config(state=tk.DISABLED)

                # Redirigir el flujo a la selección del DataFrame
                self.seleccionar_dataframe(df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, output_path)

                # Actualizar la barra de progreso
                progress_var.set(100)

            except Exception as e:
                messagebox.showerror("Error", f"Error al actualizar los datos: {e}")
            finally:
                progress_bar.stop()
                progress_window.destroy()

        # Crear una ventana de progreso
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Actualizando Datos")

        tk.Label(progress_window, text="Actualizando datos, por favor espere...").pack(pady=10)

        # Crear una barra de progreso
        progress_var = tk.IntVar()
        progress_bar = ttk.Progressbar(progress_window, orient="horizontal", length=300, mode="determinate", variable=progress_var)
        progress_bar.pack(pady=20)
        progress_bar.start()

        # Crear una etiqueta para mostrar el nombre de la tabla
        progress_label = tk.Label(progress_window, text="")
        progress_label.pack(pady=10)

        # Ejecutar la actualización de datos en un hilo separado para no bloquear la interfaz de usuario
        threading.Thread(target=run_update).start()

    def seleccionar_dataframe(self, df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, output_path):
        # Crear una ventana de selección
        selection_window = tk.Toplevel(self.root)
        selection_window.title("ELIJE QUE DATOS QUIERES ANALIZAR (EMBOLSADO/IQF)")

        # Variable para almacenar la selección del usuario
        user_choice = tk.StringVar(value="EMBOLSADO")

        # Crear botones de radio para la selección
        tk.Radiobutton(selection_window, text="EMBOLSADO", variable=user_choice, value="EMBOLSADO").pack(anchor=tk.W)
        tk.Radiobutton(selection_window, text="IQF", variable=user_choice, value="IQF").pack(anchor=tk.W)

        # Función para manejar la selección y cerrar la ventana de selección
        def on_select():
            selection_window.destroy()
            if user_choice.get() == "EMBOLSADO":
                datos_seleccionados = df_embolsado
                nombre_datos = "DATOS EMBOLSADO"
            elif user_choice.get() == "IQF":
                datos_seleccionados = df_iqf
                nombre_datos = "DATOS IQF"
            else:
                messagebox.showerror("Error", "Selección inválida. Por favor, elija 'EMBOLSADO' o 'IQF'.")
                return

            # Crear una ventana para ingresar el rango de fechas
            date_window = tk.Toplevel(self.root)
            date_window.title("Ingrese el rango de fechas")

            tk.Label(date_window, text="Fecha inicial:").pack()
            fecha_inicial_entry = DateEntry(date_window, date_pattern='yyyy-mm-dd', maxdate=fecha_mas_reciente)
            fecha_inicial_entry.pack()

            tk.Label(date_window, text="Fecha final:").pack()
            fecha_final_entry = DateEntry(date_window, date_pattern='yyyy-mm-dd', maxdate=fecha_mas_reciente)
            fecha_final_entry.pack()

            def on_date_select():
                fecha_inicial = fecha_inicial_entry.get()
                fecha_final = fecha_final_entry.get()
                date_window.destroy()

                # Filtrar el DataFrame basado en el rango de fechas
                df_filtrado = datos_seleccionados[(datos_seleccionados['fecha'] >= fecha_inicial) & (datos_seleccionados['fecha'] <= fecha_final)]

                # Crear una ventana de selección de líneas
                line_selection_window = tk.Toplevel(self.root)
                line_selection_window.title("Selecciona las líneas que deseas analizar")

                # Obtener las líneas únicas del DataFrame filtrado
                lineas_unicas = df_filtrado['línea'].unique()

                # Variables para almacenar las selecciones del usuario
                lineas_seleccionadas = {linea: tk.BooleanVar() for linea in lineas_unicas}

                # Crear checkbuttons para cada línea
                for linea in lineas_unicas:
                    tk.Checkbutton(line_selection_window, text=linea, variable=lineas_seleccionadas[linea]).pack(anchor=tk.W)

                # Función para manejar la selección de líneas y cerrar la ventana de selección de líneas
                def on_line_select():
                    line_selection_window.destroy()
                    # Filtrar el DataFrame basado en las líneas seleccionadas
                    lineas_filtradas = [linea for linea, seleccionado in lineas_seleccionadas.items() if seleccionado.get()]
                    if lineas_filtradas:
                        datos_filtrados = df_filtrado[df_filtrado['línea'].isin(lineas_filtradas)]
                    else:
                        datos_filtrados = df_filtrado

                    # Mostrar los datos filtrados en una nueva ventana
                    data_window = tk.Toplevel(self.root)
                    data_window.title("Datos Filtrados")

                    # Crear un Treeview para mostrar los datos
                    tree = ttk.Treeview(data_window)
                    tree.pack(expand=True, fill='both')

                    # Añadir barras de desplazamiento
                    vsb = ttk.Scrollbar(data_window, orient="vertical", command=tree.yview)
                    hsb = ttk.Scrollbar(data_window, orient="horizontal", command=tree.xview)
                    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
                    vsb.pack(side='right', fill='y')
                    hsb.pack(side='bottom', fill='x')

                    # Definir las columnas
                    tree["columns"] = list(datos_filtrados.columns)
                    tree["show"] = "headings"

                    # Crear encabezados de columna
                    for col in tree["columns"]:
                        tree.heading(col, text=col)

                    # Insertar los datos en el Treeview
                    for index, row in datos_filtrados.iterrows():
                        tree.insert("", "end", values=list(row))

                    # Ajustar el tamaño de las columnas
                    for col in tree["columns"]:
                        max_len = max(datos_filtrados[col].astype(str).apply(len).max(), len(col))
                        tree.column(col, width=max_len * 10)

                    # Función para exportar los datos a un archivo de Excel
                    def exportar_excel():
                        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
                        if file_path:
                            datos_filtrados.to_excel(file_path, index=True)
                            messagebox.showinfo("Éxito", f"Datos exportados correctamente a {file_path}")

                    # Botón para cerrar la ventana de datos
                    def cerrar_ventana():
                        data_window.destroy()

                    # Botón para salir del sistema
                    def salir_sistema():
                        self.root.quit()

                    tk.Button(data_window, text="Exportar a Excel", command=exportar_excel).pack(side=tk.LEFT, padx=10, pady=10)
                    tk.Button(data_window, text="Cerrar", command=cerrar_ventana).pack(side=tk.LEFT, padx=10, pady=10)
                    tk.Button(data_window, text="Salir del Sistema", command=salir_sistema).pack(side=tk.RIGHT, padx=10, pady=10)

                    # Guardar el DataFrame filtrado en un archivo Excel
                    output_path_filtrado = f'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\resultados_{nombre_datos.lower().replace(" ", "_")}.xlsx'
                    datos_filtrados.to_excel(output_path_filtrado, index=True)

                    messagebox.showinfo("Éxito", f"Datos actualizados y guardados correctamente.\nFecha más antigua: {fecha_mas_antigua}\nFecha más reciente: {fecha_mas_reciente}\nDatos seleccionados: {nombre_datos}\nLíneas seleccionadas: {lineas_filtradas}")

                # Botón para confirmar la selección de líneas
                tk.Button(line_selection_window, text="Seleccionar Líneas", command=on_line_select).pack()

            # Botón para confirmar la selección de fechas
            tk.Button(date_window, text="Seleccionar Fechas", command=on_date_select).pack()

        # Botón para confirmar la selección
        tk.Button(selection_window, text="Seleccionar", command=on_select).pack()

        # Mostrar los primeros 10 registros de df_calidad
        self.mostrar_df_calidad(df_calidad)

    def mostrar_df_calidad(self, df_calidad):
        # Crear una ventana para mostrar los primeros 10 registros de df_calidad
        calidad_window = tk.Toplevel(self.root)
        calidad_window.title("Primeros 10 registros de base_calidad_organizada")

        # Crear un Treeview para mostrar los datos
        tree = ttk.Treeview(calidad_window)
        tree.pack(expand=True, fill='both')

        # Definir las columnas
        tree["columns"] = list(df_calidad.columns)
        tree["show"] = "headings"

        # Crear encabezados de columna
        for col in tree["columns"]:
            tree.heading(col, text=col)

        # Insertar los datos en el Treeview
        for index, row in df_calidad.head(10).iterrows():
            tree.insert("", "end", values=list(row))

if __name__ == "__main__":
    root = tk.Tk()
    app = AnalisisEmbolsadoApp(root)
    root.mainloop()