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
        self.btn_actualizar = tk.Button(self.root, text="Actualizar datos", command=self.actualizar_datos)
        self.btn_actualizar.pack(pady=20)

    def actualizar_datos(self):
        def run_update():
            try:
                conn_params = {
                    'host': '128.254.204.227',
                    'dbname': 'marbran',  # Añadir el nombre de la base de datos
                    'user': 'marbran',
                    'password': 'geektv2020',
                    'options': '-c client_encoding=UTF8'
                }
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()

                progress_label.config(text="Leyendo datos de la tabla: sistfiles_sistema_produccion")
                query_produccion = "SELECT * FROM sistfiles_sistema_produccion"
                cur.execute(query_produccion)
                data_produccion = cur.fetchall()
                columns_produccion = [desc[0] for desc in cur.description]
                df_produccion = pd.DataFrame(data_produccion, columns=columns_produccion)

                df_produccion['clave_producto'] = df_produccion['cvepdto'].str[-2:]
                df_produccion['cvepdto'] = df_produccion['cvepdto'].str[:-2]
                df_produccion.set_index('cvepdto', inplace=True)

                progress_label.config(text="Leyendo datos de la tabla: base_calidad_organizada")
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
                    fecha_mas_antigua = df_produccion['fecha'].min()
                    fecha_mas_reciente = df_produccion['fecha'].max()
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

                progress_var.set(20)
                self.btn_actualizar.config(state=tk.DISABLED)
                self.seleccionar_dataframe(df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path)
                progress_var.set(100)

            except Exception as e:
                messagebox.showerror("Error", f"Error al actualizar los datos: {e}")
            finally:
                progress_bar.stop()
                progress_window.destroy()

        progress_window = tk.Toplevel(self.root)
        progress_window.title("Actualizando Datos")
        tk.Label(progress_window, text="Actualizando datos, por favor espere...").pack(pady=10)
        progress_var = tk.IntVar()
        progress_bar = ttk.Progressbar(progress_window, orient="horizontal", length=300, mode="determinate", variable=progress_var)
        progress_bar.pack(pady=20)
        progress_bar.start()
        progress_label = tk.Label(progress_window, text="")
        progress_label.pack(pady=10)
        threading.Thread(target=run_update).start()

    def seleccionar_dataframe(self, df_embolsado, df_iqf, fecha_mas_antigua, fecha_mas_reciente, df_calidad, df_material_Cabeza, output_path):
        selection_window = tk.Toplevel(self.root)
        selection_window.title("ELIJE QUE DATOS QUIERES ANALIZAR (EMBOLSADO/IQF)")
        user_choice = tk.StringVar(value="EMBOLSADO")
        tk.Radiobutton(selection_window, text="EMBOLSADO", variable=user_choice, value="EMBOLSADO").pack(anchor=tk.W)
        tk.Radiobutton(selection_window, text="IQF", variable=user_choice, value="IQF").pack(anchor=tk.W)

        def on_select():
            selection_window.destroy()
            if user_choice.get() == "EMBOLSADO":
                datos_seleccionados = df_embolsado
                nombre_datos = "DATOS EMBOLSADO"
                # Ligar df_embolsado con df_material_Cabeza
                datos_seleccionados = datos_seleccionados.merge(df_material_Cabeza, left_on='cvepdto', right_on='cve_producto', how='left')
                
                # Crear el DataFrame de porcentajes
                df_porcentajes = df_calidad.copy()

                # Mostrar el DataFrame de porcentajes en una nueva ventana
                porcentajes_window = tk.Toplevel(self.root)
                porcentajes_window.title("DataFrame de Porcentajes")
                tree_porcentajes = ttk.Treeview(porcentajes_window)
                tree_porcentajes.pack(expand=True, fill='both')
                vsb_porcentajes = ttk.Scrollbar(porcentajes_window, orient="vertical", command=tree_porcentajes.yview)
                hsb_porcentajes = ttk.Scrollbar(porcentajes_window, orient="horizontal", command=tree_porcentajes.xview)
                tree_porcentajes.configure(yscrollcommand=vsb_porcentajes.set, xscrollcommand=hsb_porcentajes.set)
                vsb_porcentajes.pack(side='right', fill='y')
                hsb_porcentajes.pack(side='bottom', fill='x')
                tree_porcentajes["columns"] = list(df_porcentajes.columns)
                tree_porcentajes["show"] = "headings"

                for col in tree_porcentajes["columns"]:
                    tree_porcentajes.heading(col, text=col)

                for index, row in df_porcentajes.iterrows():
                    tree_porcentajes.insert("", "end", values=list(row))

                for col in tree_porcentajes["columns"]:
                    max_len = max(df_porcentajes[col].astype(str).apply(len).max(), len(col))
                    tree_porcentajes.column(col, width=max_len * 10)
            else:
                datos_seleccionados = df_iqf
                nombre_datos = "DATOS IQF"

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
                df_filtrado = datos_seleccionados[(datos_seleccionados['fecha'] >= fecha_inicial) & (datos_seleccionados['fecha'] <= fecha_final)]

                line_selection_window = tk.Toplevel(self.root)
                line_selection_window.title("Selecciona las líneas que deseas analizar")
                lineas_unicas = df_filtrado['línea'].unique()
                lineas_seleccionadas = {linea: tk.BooleanVar() for linea in lineas_unicas}

                for linea in lineas_unicas:
                    tk.Checkbutton(line_selection_window, text=linea, variable=lineas_seleccionadas[linea]).pack(anchor=tk.W)

                def on_line_select():
                    line_selection_window.destroy()
                    lineas_filtradas = [linea for linea, seleccionado in lineas_seleccionadas.items() if seleccionado.get()]
                    datos_filtrados = df_filtrado[df_filtrado['línea'].isin(lineas_filtradas)] if lineas_filtradas else df_filtrado

                    data_window = tk.Toplevel(self.root)
                    data_window.title("Datos Filtrados")
                    tree = ttk.Treeview(data_window)
                    tree.pack(expand=True, fill='both')
                    vsb = ttk.Scrollbar(data_window, orient="vertical", command=tree.yview)
                    hsb = ttk.Scrollbar(data_window, orient="horizontal", command=tree.xview)
                    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
                    vsb.pack(side='right', fill='y')
                    hsb.pack(side='bottom', fill='x')
                    tree["columns"] = list(datos_filtrados.columns)
                    tree["show"] = "headings"

                    for col in tree["columns"]:
                        tree.heading(col, text=col)

                    for index, row in datos_filtrados.iterrows():
                        tree.insert("", "end", values=list(row))

                    for col in tree["columns"]:
                        max_len = max(datos_filtrados[col].astype(str).apply(len).max(), len(col))
                        tree.column(col, width=max_len * 10)

                    def exportar_excel():
                        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
                        if file_path:
                            datos_filtrados.to_excel(file_path, index=True)
                            messagebox.showinfo("Éxito", f"Datos exportados correctamente a {file_path}")

                    def cerrar_ventana():
                        data_window.destroy()

                    def salir_sistema():
                        self.root.quit()

                    tk.Button(data_window, text="Exportar a Excel", command=exportar_excel).pack(side=tk.LEFT, padx=10, pady=10)
                    tk.Button(data_window, text="Cerrar", command=cerrar_ventana).pack(side=tk.LEFT, padx=10, pady=10)
                    tk.Button(data_window, text="Salir del Sistema", command=salir_sistema).pack(side=tk.RIGHT, padx=10, pady=10)

                    output_path_filtrado = f'C:\\Users\\agomez\\OneDrive - MarBran SA de CV\\1.5 analisis embolsados\\resultados_{nombre_datos.lower().replace(" ", "_")}.xlsx'
                    datos_filtrados.to_excel(output_path_filtrado, index=True)

                    messagebox.showinfo("Éxito", f"Datos actualizados y guardados correctamente.\nFecha más antigua: {fecha_mas_antigua}\nFecha más reciente: {fecha_mas_reciente}\nDatos seleccionados: {nombre_datos}\nLíneas seleccionadas: {lineas_filtradas}")

                tk.Button(line_selection_window, text="Seleccionar Líneas", command=on_line_select).pack()

            tk.Button(date_window, text="Seleccionar Fechas", command=on_date_select).pack()

        tk.Button(selection_window, text="Seleccionar", command=on_select).pack()
        self.mostrar_df_calidad(df_calidad)

    def mostrar_df_calidad(self, df_calidad):
        calidad_window = tk.Toplevel(self.root)
        calidad_window.title("Primeros 10 registros de base_calidad_organizada")
        tree = ttk.Treeview(calidad_window)
        tree.pack(expand=True, fill='both')
        tree["columns"] = list(df_calidad.columns)
        tree["show"] = "headings"

        for col in tree["columns"]:
            tree.heading(col, text=col)

        for index, row in df_calidad.head(10).iterrows():
            tree.insert("", "end", values=list(row))

if __name__ == "__main__":
    root = tk.Tk()
    app = AnalisisEmbolsadoApp(root)
    root.mainloop()