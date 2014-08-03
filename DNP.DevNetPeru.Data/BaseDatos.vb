Imports System
Imports System.Configuration
Imports System.Data
Imports System.Data.SqlClient

''' <summary>
''' Nombre       : BaseDatos
''' Descripción  : Metodos de manipulación para trabajar con las bases de datos SQLServer
''' </summary>
''' <remarks>
''' Creacion     : 11/07/2014 Daniel La Torre L.
''' Creacion     : 11/07/2014 Rocío Huisa M.
''' Modificacion : 
''' </remarks>
Public Class BaseDatos

    ''' <summary>
    ''' Devuelve un objeto SqlConnection con la conexion abierta,
    ''' desde la cadena de conexion Default para la aplicacion 
    ''' (ConnectionString)
    ''' </summary>    
    Public Shared Function GetConnection() As SqlConnection
        Using connection As New SqlConnection(GetConnectionString())
            connection.Open()
            Return connection
        End Using
    End Function

    ''' <summary>
    ''' Devuelve un objeto SqlConnection con la conexion abierta,
    ''' desde la cadena de conexion proporcionada por el usuario que debe estar en el archivo de configuracion
    ''' (ConnectionString)
    ''' </summary>
    Public Shared Function GetConnection(ByVal connectionString As String) As SqlConnection
        Using connection As New SqlConnection(GetConnectionString(connectionString))
            connection.Open()
            Return connection
        End Using
    End Function

    ''' <summary>
    ''' Devuelve la cadena de conexion default del archivo de configuracion
    ''' </summary>
    Public Shared Function GetConnectionString() As String
        Return ConfigurationManager.ConnectionStrings("ConnectionString").ConnectionString
    End Function

    Public Shared Function GetConnectionString(ByVal connectionString As String) As String
        Return ConfigurationManager.ConnectionStrings(connectionString).ConnectionString
    End Function

    ''' <summary>
    ''' Lectura de datos mediante un objeto SqlDataReader.
    ''' </summary>
    ''' <param name="cmd">
    ''' Comando (SqlCommand) que debe especificar:
    ''' 1. CommandType
    ''' 2. CommandName
    ''' 3. Parameters (si los hubiera)
    ''' </param>
    ''' <returns>SqlDataReader</returns>
    Public Shared Function GetDataReader(ByVal cmd As SqlCommand) As SqlDataReader

        Call ValidCommand(cmd)
        cmd.Connection = GetConnection()
        Return cmd.ExecuteReader(CommandBehavior.CloseConnection)

    End Function

    ''' <summary>
    ''' Metodo utilizado para la lectura de datos. Devuelve un objeto OracleDataReader.
    ''' </summary>
    ''' <param name="instruccionSQL">instruccion Sql a ejecutar</param>
    Public Shared Function GetDataReader(ByVal instruccionSQL As String) As SqlDataReader

        Dim cmd As New SqlCommand
        cmd.Connection = GetConnection()
        cmd.CommandType = CommandType.Text
        cmd.CommandText = instruccionSQL

        Return cmd.ExecuteReader(CommandBehavior.CloseConnection)
    End Function

    ''' <summary>
    ''' Metodo utilizado para obtener un objeto DataSet.
    ''' </summary>
    ''' <param name="cmd">
    ''' Comando (SqlCommand) que debe especificar:
    ''' 1. CommandType
    ''' 2. CommandName
    ''' 3. Parameters (si los hubiera)
    ''' </param> 
    Public Shared Function GetDataSet(ByVal cmd As SqlCommand) As DataSet

        Call ValidCommand(cmd)

        Dim da As New SqlDataAdapter
        Dim ds As New DataSet

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            da.SelectCommand = cmd
            da.Fill(ds)
        End Using

        Return ds

    End Function

    ''' <summary>
    ''' Metodo utilizado para obtener un objeto DataSet.
    ''' </summary>
    ''' <param name="instruccionSQL">Cadena SQL para ejecutar el objeto DataAdapter.</param>
    Public Shared Function GetDataSet(ByVal instruccionSQL As String) As DataSet
        Dim cmd As New SqlCommand
        Dim da As New SqlDataAdapter
        Dim ds As New DataSet

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            cmd.CommandType = CommandType.Text
            cmd.CommandText = instruccionSQL
            da.SelectCommand = cmd
            da.Fill(ds)
        End Using

        Return ds

    End Function

    ''' <summary>
    ''' Metodo utilizado para obtener un unico valor mediante una consulta.
    ''' </summary>
    ''' <param name="cmd">
    ''' Comando (SqlCommand) que debe especificar:
    ''' 1. CommandType
    ''' 2. CommandName
    ''' 3. Parameters (si los hubiera)
    ''' </param> 
    Public Shared Function GetDataScalar(ByVal cmd As SqlCommand) As Object

        Call ValidCommand(cmd)

        Dim da As New SqlDataAdapter
        Dim ds As New DataSet
        Dim valorScalar As Object

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            valorScalar = cmd.ExecuteScalar
        End Using

        Return valorScalar
    End Function

    ''' <summary>
    ''' Metodo utilizado para obtener un unico valor mediante una consulta.
    ''' </summary>
    ''' <param name="instruccionSQL">Cadena SQL para ejecutar la consulta</param>
    Public Shared Function GetDataScalar(ByVal instruccionSQL As String) As Object
        Dim cmd As New SqlCommand
        Dim da As New SqlDataAdapter
        Dim ds As New DataSet
        Dim valorScalar As Object

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            cmd.CommandType = CommandType.Text
            cmd.CommandText = instruccionSQL
            valorScalar = cmd.ExecuteScalar
        End Using

        Return valorScalar
    End Function

    ''' <summary>
    ''' Método utilizado para ejecutar las acciones de inserción, edición y eliminación.
    ''' </summary>
    ''' <param name="cmd">
    ''' Comando (sqlCommand) que debe especificar:
    ''' 1. CommandType
    ''' 2. CommandName
    ''' 3. Parameters (si los hubiera)
    ''' </param>
    ''' <returns>Número de registros afectados</returns>
    Public Shared Function Execute(ByVal cmd As SqlCommand) As Integer
        Call ValidCommand(cmd)

        Dim nroRegistos As Integer

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            nroRegistos = cmd.ExecuteNonQuery()
        End Using

        Return nroRegistos
    End Function

    ''' <summary>
    ''' Método utilizado para ejecutar las acciones de inserción, edición y eliminación en una transaccion.
    ''' </summary>
    ''' <param name="cmd">
    ''' Comando (SqlCommand) que debe especificar:
    ''' 1. CommandType
    ''' 2. CommandName
    ''' 3. Parameters (si los hubiera)
    ''' 4. Transaction
    ''' </param>
    ''' <returns>Número de registros afectados</returns>
    Public Shared Function Execute(ByVal cmd As SqlCommand, ByVal transaction As SqlTransaction) As Integer
        Call ValidCommand(cmd)

        Dim nroRegistros As Integer
        cmd.Connection = transaction.Connection
        'cmd.Transaction = transaction
        nroRegistros = cmd.ExecuteNonQuery()

        Return nroRegistros
    End Function

    ''' <summary>
    ''' Método utilizado para ejecutar las acciones de inserción, edición, eliminación.
    ''' </summary>
    ''' <param name="instruccionSQL">Cadena SQL para ejecutar la acción.</param>
    ''' <returns>Número de registros afectados</returns>
    Public Shared Function Execute(ByVal instruccionSQL As String) As Integer
        Dim cmd As New SqlCommand
        Dim nroRegistros As Integer

        Using cn As SqlConnection = GetConnection()
            cmd.Connection = cn
            cmd.CommandType = CommandType.Text
            cmd.CommandText = instruccionSQL
            nroRegistros = cmd.ExecuteNonQuery()
        End Using

        Return nroRegistros
    End Function

    ''' <summary>
    ''' Valida los datos basicos de un comando
    ''' </summary>
    Private Shared Function ValidCommand(ByVal cmd As SqlCommand) As Boolean
        If cmd Is Nothing Then Throw New ArgumentException("Debe proporcionar un objecto OracleCommand")
        If String.IsNullOrEmpty(cmd.CommandText) Then Throw New ArgumentException("Debe proporcionar el nombre del procedimiento almacenado o instruccion SQL")

        Return True
    End Function

#Region "Funciones de manipulacion de datos en DataReader's"

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Número del campo del objeto DataReader.</param>
    Public Shared Function GetDateValue(ByRef dr As IDataReader, ByVal campo As Integer) As Nullable(Of Date)
        If (dr.IsDBNull(campo)) Then
            Return Nothing
        Else
            Return dr.GetDateTime(campo)
        End If
    End Function

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Nombre del campo del objeto DataReader.</param>
    Public Shared Function GetDateValue(ByRef dr As IDataReader, ByVal campo As String) As Nullable(Of Date)
        If (dr(campo) Is DBNull.Value) Then
            Return Nothing
        Else
            Return dr.GetDateTime(campo)
        End If
    End Function

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' De ser así, devuelve un mínimo valor, en este caso cero (0).
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Número del campo del objeto DataReader.</param>
    Public Shared Function GetNumericValue(ByRef dr As IDataReader, ByVal campo As Integer) As System.Object
        If (dr.IsDBNull(campo)) Then
            Return 0
        Else
            Return dr.GetValue(campo)
        End If
    End Function

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' De ser así, devuelve un mínimo valor, en este caso cero (0).
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Nombre del campo del objeto DataReader.</param>
    Public Shared Function GetNumericValue(ByRef dr As IDataReader, ByVal campo As String) As System.Object
        If (dr.IsDBNull(campo)) Then
            Return 0
        Else
            Return dr(campo)
        End If
    End Function

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' De ser así, devuelve un mínimo valor, en este caso valor vacío ("").
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Número del campo del objeto DataReader.</param>
    Public Shared Function GetStringValue(ByRef dr As IDataReader, ByVal campo As Integer) As String
        If (dr.IsDBNull(campo)) Then
            Return ""
        Else
            Return dr(campo).ToString
        End If
    End Function

    ''' <summary>
    ''' Método útil para definir si el campo del objeto reader está nulo.
    ''' De ser así, devuelve un mínimo valor, en este caso valor vacío ("").
    ''' </summary>
    ''' <param name="dr">Objeto DataReader.</param>
    ''' <param name="campo">Nombre del campo del objeto DataReader.</param>
    Public Shared Function GetStringValue(ByRef dr As IDataReader, ByVal campo As String) As String
        If (dr(campo) Is DBNull.Value) Then
            Return ""
        Else
            Return dr(campo).ToString
        End If
    End Function

#End Region

End Class

