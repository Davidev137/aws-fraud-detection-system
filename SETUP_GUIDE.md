# Guía de Configuración de Credenciales AWS

Esta guía detalla cómo obtener y configurar las credenciales necesarias para ejecutar el proyecto.

## Paso 1: Crear Usuario en AWS IAM

1.  Inicia sesión en tu consola de AWS: [https://console.aws.amazon.com/](https://console.aws.amazon.com/)
2.  En la barra de búsqueda superior, escribe **IAM** y selecciona el servicio.
3.  En el menú izquierdo, haz clic en **Users** (Usuarios).
4.  Haz clic en el botón **Create user** (Crear usuario).
5.  **User details**:
    *   User name: `fraud-demo-admin` (o el nombre que prefieras).
    *   Haz clic en **Next**.
6.  **Permissions**:
    *   Selecciona **Attach policies directly**.
    *   En la búsqueda de permisos, escribe `AdministratorAccess`.
    *   Marca la casilla al lado de **AdministratorAccess**.
    *   *Nota: Para un entorno de producción real usaríamos permisos más restrictivos, pero para esta demo y aprendizaje, Admin evita errores de permisos.*
    *   Haz clic en **Next**.
7.  Revisa y haz clic en **Create user**.

## Paso 2: Generar Claves de Acceso (Access Keys)

1.  En la lista de usuarios, haz clic en el nombre del usuario que acabas de crear (`fraud-demo-admin`).
2.  Ve a la pestaña **Security credentials** (Credenciales de seguridad).
3.  Baja hasta la sección **Access keys** y haz clic en **Create access key**.
4.  Selecciona **Command Line Interface (CLI)**.
5.  Marca la casilla de confirmación "I understand..." y haz clic en **Next**.
6.  (Opcional) Pon una etiqueta como "Laptop Demo".
7.  Haz clic en **Create access key**.
8.  **¡IMPORTANTE!**: Verás tu **Access key** y **Secret access key**.
    *   Copia ambas en un lugar seguro temporalmente.
    *   O descarga el archivo `.csv`.
    *   *El Secret Key solo se muestra una vez. Si lo pierdes, tendrás que crear una llave nueva.*

## Paso 3: Configurar AWS CLI en tu Computadora

1.  Abre tu terminal (PowerShell, CMD o Terminal).
2.  Ejecuta el siguiente comando:
    ```bash
    aws configure
    ```
3.  Ingresa los datos cuando te los pida:
    *   **AWS Access Key ID**: [Pega tu Access Key aquí]
    *   **AWS Secret Access Key**: [Pega tu Secret Key aquí]
    *   **Default region name**: `us-east-1`
        *   *Es muy importante usar `us-east-1` (N. Virginia) porque los scripts están configurados para esta región por defecto.*
    *   **Default output format**: `json`

## Paso 4: Verificar

Ejecuta este comando para probar que todo funciona:

```bash
aws sts get-caller-identity
```

Deberías ver una respuesta JSON con tu `UserId`, `Account` y `Arn`. Si ves esto, ¡estás listo!
