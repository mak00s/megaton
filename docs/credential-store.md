# Credential Store（認証ストア）

Available from Megaton 2.6.0.

1台のMacに1つの置き場（`~/.config/megaton`、または `$MEGATON_HOME`）を作り、Googleの資格情報をすべてそこに集める。コードやAIエージェントはファイルパスではなく**識別子**（`account="sim"`、`sa="corp-ga"`）で資格情報を引くため、repoが変わっても、エージェントが変わっても、同じ手順で認証できる。

## 2系統に分ける理由

| 用途 | 資格情報 | 失効 | 識別子 |
|---|---|---|---|
| Workspaceの中身（Docs・Drive・Sheets・Gmail・GA4・GSC・GTM） | ユーザーOAuthトークン（`users/<email>.json`） | しない（Google Cloudスコープを含めない限り、Workspaceのセッション制御の対象外） | `account` |
| Google Cloud（BigQuery・Cloud Run など） | サービスアカウント鍵（`sa/<name>.json`） | しない | `sa` |

gcloudのユーザー認証（`gcloud auth login`）はWorkspace管理者のセッション制御で毎日失効する。APIを叩く用途にはこのストアを使い、gcloudはGCPの操作だけに使う。

**前提**: ユーザートークンが失効しないのは、OAuthクライアントのプロジェクトの公開ステータスが「本番」（または Workspace 内部向け）のとき。「テスト」状態の外部向けアプリではリフレッシュトークンが7日で失効する。`megaton auth check` が7日を超えて `ok` なら本番状態と判断できる。

**保存される内容**: トークンの `scopes` は Google が実際に許可した範囲（同意画面で外した分は含まない）。`login` は要求した範囲が許可されなかったとき警告して終了コード1を返す。`import --user-token` は所有者をオンラインで必ず確認し、確認できないトークンは取り込まない。書き込みは一時ファイル＋置換（原子的）。

## 配置

```
~/.config/megaton/
  credentials.toml        # 別名の台帳（手で編集）
  oauth-client.json       # `megaton auth login` が使うOAuthクライアント（デスクトップ型）
  users/sim@example.com.json
  sa/sa_corp-1872_py-ga-gsc-api.json
```

`credentials.toml`:

```toml
[oauth]
client = "oauth-client.json"

[users]
sim = "sim@example.com"

[sa]
corp-ga = "sa_corp-1872_py-ga-gsc-api.json"
```

鍵ファイルは各Macにコピーして置く（Dropbox等で同期しない）。ディレクトリとファイルは `0700` / `0600`。

## CLI

```bash
megaton auth init                                   # 置き場と台帳の雛形を作る
megaton auth import --oauth-client client.json      # OAuthクライアントを登録
megaton auth import --sa key.json --name corp-ga    # SA鍵を取り込む
megaton auth import --user-token old_token.json     # 既存トークンを取り込む（メールを検証）
megaton auth login --account sim@example.com --scopes docs,drive,sheets
megaton auth list                                   # 何があるか
megaton auth check                                  # 更新できるか（失敗時は直すコマンドを表示）
megaton auth token --account sim --scopes docs      # curl用のアクセストークン
megaton auth path --sa corp-ga                      # gspread等にパスを渡したいとき
megaton auth scopes                                 # スコープ別名の一覧
```

`login` は既存トークンのスコープに要求スコープを足して再認可する（1ユーザー1トークン、段階的に拡張）。ブラウザでサインインしたアカウントが `--account` と違えば保存しない。

## Python

```python
from megaton import Megaton
from megaton.auth import CredentialStore

mg = Megaton(sa="corp-ga", headless=True)                       # GA4/GSC/Sheets
mg = Megaton(account="sim", scopes=["sheets"], headless=True)   # ユーザーのトークン

store = CredentialStore()
creds = store.user_credentials("sim", scopes="docs,drive")      # google.oauth2.credentials.Credentials
token = store.user_access_token("sim", "docs")                  # str
path = store.service_account_path("corp-ga")                    # Path（gspread.service_account(filename=path)）
sa_creds = store.service_account_credentials("corp-ga", "sheets,drive")
```

`user_credentials` は要求スコープがトークンに無ければ `CredentialStoreError` を投げる。`hint` に実行すべき `megaton auth login ...` が入るので、エージェントはそれをそのまま案内する。スコープは要求で広がらない（再認可は必ず人がブラウザで行う）。

## curl から使う

```bash
TOKEN=$(megaton auth token --account sim --scopes docs)
curl -s -H "Authorization: Bearer $TOKEN" "https://docs.googleapis.com/v1/documents/<id>?includeTabsContent=true"
```

## スコープ別名

`docs`, `drive`, `drive.file`, `sheets`, `gmail.readonly`, `gmail.modify`, `gmail.compose`, `calendar`, `analytics`, `analytics.edit`, `webmasters`, `tagmanager`, `tagmanager.edit`, `bigquery`, `cloud-platform` など。`megaton auth scopes` で全一覧。上位スコープ（`drive` は `sheets`/`docs` を含む、`gmail.modify` は `gmail.readonly` を含む）は自動で充足と判定する。

## 別のMacへの複製

1. 新しいMacで `pip install megaton` と `megaton auth init`
2. `credentials.toml`・`oauth-client.json`・`users/`・`sa/` を安全な経路（AirDrop等）でコピー
3. `megaton auth check` で全ユーザーが `ok` になることを確認
